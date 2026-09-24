#!/usr/bin/env python3
"""Meme Pulse 수집기.

서울 실시간 도시데이터(인구·연령·혼잡·도로·날씨·대기), 에어코리아 측정소 실시간 농도,
네이버 데이터랩 연령별 검색 추이, (선택) X 게시물 수를 모아
서울 주요 장소 전체를 주기적으로 훑어 평소보다 사람이 몰린 곳(밈 후보)을 스스로 찾고,
구글 트렌드 한국 급상승 검색어와 장소 이름을 맞춰 본 뒤, 후보를 자동으로 추적 대상에 추가한다.
결과는
data/history.json 과 data/data.js 로 저장한다. 표준 라이브러리만 사용한다.

  python collector/collect.py            한 번 수집
  python collector/collect.py --loop 600 10분마다 계속 수집 (Ctrl+C로 종료)
  python collector/collect.py --check    인증키·설정 점검만
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request

KST = dt.timezone(dt.timedelta(hours=9))
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATA_DIR = os.path.join(ROOT, "data")
SCHEMA = 1

# 네이버 데이터랩 연령 코드 → 분석용 연령대
AGE_BANDS = {
    "전체": [],
    "10대": ["1", "2"],          # 0–18세 (코드 1: 0–12, 2: 13–18)
    "20대": ["3", "4"],
    "30대": ["5", "6"],
    "40대": ["7", "8"],
    "50대+": ["9", "10", "11"],
}


# ---------------------------------------------------------------- 공통
def now():
    return dt.datetime.now(KST)


def iso(d):
    return d.strftime("%Y-%m-%dT%H:%M")


def log(msg):
    print(f"[{now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_keys():
    """환경변수 우선, 없으면 collector/keys.env 파일(KEY=VALUE)에서 읽는다."""
    keys = {}
    path = os.path.join(HERE, "keys.env")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    keys[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("SEOUL_API_KEY", "AIRKOREA_API_KEY", "NAVER_CLIENT_ID",
              "NAVER_CLIENT_SECRET", "X_BEARER_TOKEN", "YOUTUBE_API_KEY",
              "IG_USER_ID", "IG_ACCESS_TOKEN", "GOOGLE_MAPS_API_KEY"):
        if os.environ.get(k):
            keys[k] = os.environ[k].strip()
    return keys


def http(url, data=None, headers=None, timeout=25):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    req.add_header("User-Agent", "MemePulse/1.0")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"HTTP {e.code}: {body}") from None
    except urllib.error.URLError as e:
        raise RuntimeError(f"접속 실패: {e.reason}") from None


def num(v):
    if v is None:
        return None
    s = str(v).replace(",", "").strip()
    if s in ("", "-", "null", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_time(s):
    """'2026-09-24 14:05', '2026-09-24 24:00', '202609241405' → 'YYYY-MM-DDTHH:MM'"""
    if not s:
        return None
    s = str(s).strip()
    if s.endswith("24:00") and len(s) >= 16:
        d = dt.datetime.strptime(s[:10], "%Y-%m-%d") + dt.timedelta(days=1)
        return d.strftime("%Y-%m-%dT00:00")
    for f, n in (("%Y-%m-%d %H:%M", 16), ("%Y-%m-%dT%H:%M", 16), ("%Y%m%d%H%M", 12)):
        try:
            return dt.datetime.strptime(s[:n], f).strftime("%Y-%m-%dT%H:%M")
        except ValueError:
            continue
    return None


def find(obj, key):
    """중첩된 dict/list에서 key의 첫 값을 찾는다 (API 응답 모양이 조금 달라도 동작)."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find(v, key)
            if r is not None:
                return r
    return None


def first_dict_with(obj, key):
    if isinstance(obj, dict):
        if key in obj and not isinstance(obj[key], (dict, list)):
            return obj
        for v in obj.values():
            r = first_dict_with(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = first_dict_with(v, key)
            if r is not None:
                return r
    return None


# ---------------------------------------------------------------- 서울 실시간 도시데이터
CONGEST = {"여유": 1, "보통": 2, "약간 붐빔": 3, "붐빔": 4}


def seoul_json(txt):
    """서울 API는 오류일 때 json 요청에도 XML을 돌려주기도 한다."""
    if txt.lstrip().startswith("<"):
        code = txt.split("<CODE>")[-1].split("<")[0] if "<CODE>" in txt else ""
        msg = txt.split("<MESSAGE>")[-1].split("<")[0] if "<MESSAGE>" in txt else txt[:120]
        raise RuntimeError(f"{code} {msg}".strip())
    return json.loads(txt)


def fetch_seoul(key, place):
    area = place["citydata_area"]
    url = f"http://openapi.seoul.go.kr:8088/{urllib.parse.quote(key)}/json/citydata/1/5/{urllib.parse.quote(area)}"
    j = seoul_json(http(url))
    city = j.get("CITYDATA")
    if not city:
        res = j.get("RESULT") or {}
        code = res.get("RESULT.CODE") or res.get("CODE")
        msg = res.get("RESULT.MESSAGE") or res.get("MESSAGE")
        raise RuntimeError(f"{area}: {code} {msg}")
    pp = first_dict_with(city.get("LIVE_PPLTN_STTS"), "PPLTN_TIME") or {}
    wt = first_dict_with(city.get("WEATHER_STTS"), "WEATHER_TIME") or {}
    road = find(city.get("ROAD_TRAFFIC_STTS"), "AVG_ROAD_DATA") or {}
    t = parse_time(pp.get("PPLTN_TIME")) or parse_time(wt.get("WEATHER_TIME")) or iso(now())
    lo, hi = num(pp.get("AREA_PPLTN_MIN")), num(pp.get("AREA_PPLTN_MAX"))
    rec = {
        "src": "seoul", "place": place["id"], "t": t,
        "pmin": lo, "pmax": hi,
        "pop": (lo + hi) / 2 if lo is not None and hi is not None else None,
        "cg": CONGEST.get(str(pp.get("AREA_CONGEST_LVL", "")).strip()),
        "age": {a: num(pp.get(f"PPLTN_RATE_{a}")) for a in ("0", "10", "20", "30", "40", "50", "60", "70")},
        "male": num(pp.get("MALE_PPLTN_RATE")),
        "resnt": num(pp.get("RESNT_PPLTN_RATE")),
        "spd": num(road.get("ROAD_TRAFFIC_SPD")),
        "tidx": road.get("ROAD_TRAFFIC_IDX"),
        "temp": num(wt.get("TEMP")), "hum": num(wt.get("HUMIDITY")),
        "wind": num(wt.get("WIND_SPD")),
        "pm25": num(wt.get("PM25")), "pm10": num(wt.get("PM10")),
        "wt": parse_time(wt.get("WEATHER_TIME")),
    }
    return [rec]


# ---------------------------------------------------------------- 에어코리아
def fetch_air(key, station, place_ids, kind):
    k = key if "%" in key else urllib.parse.quote(key, safe="")
    url = ("https://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty"
           f"?serviceKey={k}&returnType=json&numOfRows=6&pageNo=1"
           f"&stationName={urllib.parse.quote(station)}&dataTerm=DAILY&ver=1.3")
    txt = http(url)
    if txt.lstrip().startswith("<"):
        msg = txt.split("<returnAuthMsg>")[-1].split("<")[0] if "returnAuthMsg" in txt else txt[:200]
        raise RuntimeError(f"{station}: {msg}")
    j = json.loads(txt)
    header = find(j, "resultCode")
    if header not in (None, "00"):
        raise RuntimeError(f"{station}: {find(j, 'resultMsg')}")
    items = find(j, "items") or []
    out = []
    for it in items:
        t = parse_time(it.get("dataTime"))
        if not t:
            continue
        for pid in place_ids:
            out.append({
                "src": kind, "place": pid, "t": t, "station": station,
                "pm25": num(it.get("pm25Value")), "pm10": num(it.get("pm10Value")),
                "no2": num(it.get("no2Value")), "o3": num(it.get("o3Value")),
                "co": num(it.get("coValue")),
            })
    if not out:
        raise RuntimeError(f"{station}: 자료 없음 (측정소 이름을 확인하세요)")
    return out


# ---------------------------------------------------------------- 네이버 데이터랩
def fetch_naver(cid, secret, groups, days):
    end = now().date()
    start = end - dt.timedelta(days=days)
    result = {}
    for gi in range(0, len(groups), 5):  # 한 요청에 키워드 그룹 최대 5개
        chunk = groups[gi:gi + 5]
        for band, ages in AGE_BANDS.items():
            body = {
                "startDate": start.isoformat(), "endDate": end.isoformat(),
                "timeUnit": "date",
                "keywordGroups": [{"groupName": g["name"], "keywords": g["keywords"][:20]} for g in chunk],
            }
            if ages:
                body["ages"] = ages
            txt = http("https://openapi.naver.com/v1/datalab/search",
                       data=json.dumps(body).encode("utf-8"),
                       headers={"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": secret,
                                "Content-Type": "application/json"})
            j = json.loads(txt)
            if "results" not in j:
                raise RuntimeError(f"네이버 응답 오류: {txt[:200]}")
            for r in j["results"]:
                g = next((x for x in chunk if x["name"] == r["title"]), None)
                entry = result.setdefault(r["title"], {"place": g["place"] if g else None,
                                                       "keywords": r.get("keywords", []), "ages": {}})
                entry["ages"][band] = [[d["period"], d["ratio"]] for d in r.get("data", [])]
            time.sleep(0.15)
    return result


# ---------------------------------------------------------------- X (선택)
def fetch_x(token, q):
    url = ("https://api.x.com/2/tweets/counts/recent?granularity=hour&query="
           + urllib.parse.quote(q["query"]))
    j = json.loads(http(url, headers={"Authorization": f"Bearer {token}"}))
    if "data" not in j:
        raise RuntimeError(f"X 응답 오류: {json.dumps(j, ensure_ascii=False)[:200]}")
    rows = []
    for d in j["data"]:
        t = dt.datetime.fromisoformat(d["start"].replace("Z", "+00:00")).astimezone(KST)
        rows.append([iso(t), d["tweet_count"]])
    return rows



# ---------------------------------------------------------------- 자동 발굴: 서울 주요 장소 전체 스캔
def scan_place(key, code):
    """장소 코드(POI001…)로 인구·연령·진행 중 행사만 가볍게 읽는다."""
    url = f"http://openapi.seoul.go.kr:8088/{urllib.parse.quote(key)}/json/citydata/1/5/{code}"
    j = seoul_json(http(url, timeout=20))
    city = j.get("CITYDATA")
    if not city:
        return None
    pp = first_dict_with(city.get("LIVE_PPLTN_STTS"), "PPLTN_TIME") or {}
    lo, hi = num(pp.get("AREA_PPLTN_MIN")), num(pp.get("AREA_PPLTN_MAX"))
    if lo is None or hi is None:
        return None
    y = (num(pp.get("PPLTN_RATE_10")) or 0) + (num(pp.get("PPLTN_RATE_20")) or 0)
    today = now().strftime("%Y-%m-%d")
    event = None
    evs = city.get("EVENT_STTS") or []
    if isinstance(evs, dict):
        evs = evs.get("EVENT_STTS") or [evs]
    for ev in evs if isinstance(evs, list) else []:
        period = str(ev.get("EVENT_PERIOD", ""))
        parts = [p.strip()[:10] for p in period.replace("~", " ~ ").split("~")]
        if len(parts) == 2 and parts[0] <= today <= parts[1]:
            event = ev.get("EVENT_NM")
            break
    return {"code": code, "name": city.get("AREA_NM") or pp.get("AREA_NM") or code,
            "t": parse_time(pp.get("PPLTN_TIME")) or iso(now()),
            "pop": (lo + hi) / 2, "cg": CONGEST.get(str(pp.get("AREA_CONGEST_LVL", "")).strip()),
            "y": y, "ev": event}


def load_json(name, default):
    path = os.path.join(DATA_DIR, name)
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def run_scan(cfg, key, hist):
    """주요 장소 전체를 훑어 평소 같은 시간대 대비 인구 배율을 계산한다."""
    d = cfg.get("discovery", {})
    scan = load_json("scan.json", {"rows": [], "codes": []})
    last = hist["status"].get("scan", {}).get("lastSuccess")
    if last and now() - dt.datetime.strptime(last, "%Y-%m-%dT%H:%M").replace(tzinfo=KST) \
            < dt.timedelta(hours=d.get("scan_every_hours", 3)) - dt.timedelta(minutes=10):
        log("전체 장소 스캔: 주기 전이라 건너뜀")
        return
    codes = scan.get("codes") or [f"POI{i:03d}" for i in range(1, d.get("max_code", 130) + 1)]
    found, rows, errs = [], [], 0
    for code in codes:
        try:
            r = scan_place(key, code)
            if r:
                rows.append(r)
                found.append(code)
        except Exception:
            errs += 1
        time.sleep(0.1)
    if not rows:
        set_status(hist, "scan", False, f"응답한 장소가 없습니다 (오류 {errs}건)")
        return
    if not scan.get("codes"):
        scan["codes"] = found            # 처음 한 번 실제 존재하는 코드만 기억
    cutoff = iso(now() - dt.timedelta(days=d.get("baseline_days", 28)))
    scan["rows"] = [r for r in scan.get("rows", []) if r["t"] >= cutoff] + rows
    save_json("scan.json", scan)

    # 평소 대비 배율: 같은 장소, 같은 평일/주말, 앞뒤 1시간 이내 시각, 최근 24시간 제외
    import statistics
    def slot(t):
        x = dt.datetime.strptime(t, "%Y-%m-%dT%H:%M")
        return x.hour, x.weekday() >= 5
    recent = iso(now() - dt.timedelta(hours=24))
    by_code = {}
    for r in scan["rows"]:
        by_code.setdefault(r["code"], []).append(r)
    cands = hist.get("discovery", {}).get("candidates", {})
    board = []
    for r in rows:
        h, we = slot(r["t"])
        base = [b for b in by_code.get(r["code"], []) if b["t"] < recent
                and slot(b["t"])[1] == we and min(abs(slot(b["t"])[0] - h), 24 - abs(slot(b["t"])[0] - h)) <= 1]
        n = len(base)
        med = statistics.median([b["pop"] for b in base]) if n >= d.get("min_baseline", 3) else None
        ymed = statistics.median([b["y"] for b in base]) if med else None
        ratio = r["pop"] / med if med else None
        item = {"code": r["code"], "name": r["name"], "t": r["t"], "pop": r["pop"], "cg": r["cg"],
                "ratio": round(ratio, 2) if ratio else None, "base_n": n,
                "dy": round(r["y"] - ymed, 1) if ymed is not None else None, "ev": r["ev"]}
        board.append(item)
        hot = ratio is not None and ratio >= d.get("ratio_threshold", 1.4) and r["pop"] >= d.get("min_pop", 2000)
        if hot:
            c = cands.setdefault(r["code"], {"code": r["code"], "name": r["name"], "first": r["t"], "hits": []})
            c["hits"] = [x for x in c["hits"] if x >= iso(now() - dt.timedelta(days=7))] + [r["t"]]
            c.update(last=r["t"], ratio=item["ratio"], dy=item["dy"], ev=r["ev"], pop=r["pop"],
                     max_ratio=max(c.get("max_ratio") or 0, item["ratio"]))
    # 14일 넘게 다시 포착되지 않은 후보는 정리
    old = iso(now() - dt.timedelta(days=14))
    cands = {k: v for k, v in cands.items() if v.get("last", "") >= old}
    board.sort(key=lambda x: -(x["ratio"] or 0))
    hist["discovery"] = {"scannedAt": iso(now()), "places": len(rows), "board": board,
                         "candidates": cands}
    set_status(hist, "scan", True, f"오류 {errs}건" if errs else "", len(rows))
    log(f"전체 장소 스캔 {len(rows)}곳, 후보 {len(cands)}곳")


def fetch_trends(hist, cfg):
    """구글 트렌드 한국 급상승 검색어 RSS (인증키 불필요)."""
    import xml.etree.ElementTree as ET
    txt = http("https://trends.google.com/trending/rss?geo=KR")
    root = ET.fromstring(txt)
    ns = {"ht": "https://trends.google.com/trending/rss"}
    names = set()
    for b in hist.get("discovery", {}).get("board", []):
        for part in b["name"].replace("(", "·").replace(")", "").split("·"):
            part = part.strip()
            if len(part) >= 2:
                names.add(part)
    for p in all_places(cfg, hist):
        names.add(p["label"].split("(")[0].strip())
    words = cfg.get("discovery", {}).get("place_words", [])
    old = {t["title"]: t for t in hist.get("trends", [])}
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        if not title:
            continue
        traffic = None
        for el in item:
            if el.tag.endswith("approx_traffic"):
                traffic = el.text
        news = [el.text for el in item.iter() if el.tag.endswith("news_item_title") and el.text][:2]
        hay = title + " " + " ".join(news)
        match = sorted({n for n in names if n in hay})
        kw = sorted({w for w in words if w in hay})
        entry = old.get(title, {"title": title, "first": iso(now())})
        entry.update(last=iso(now()), traffic=traffic, news=news, places=match, words=kw)
        old[title] = entry
    keep = iso(now() - dt.timedelta(days=7))
    hist["trends"] = sorted([t for t in old.values() if t["last"] >= keep], key=lambda t: t["last"], reverse=True)[:150]


def auto_track(cfg, hist):
    """(선택) 인구 급증만으로 포착된 후보를 추적한다. 기본은 꺼 두고, 키워드와 겹칠 때만 확인 표시에 쓴다."""
    d = cfg.get("discovery", {})
    if not d.get("population_auto_track", False):
        hist["auto_places"] = []
        return
    fixed = {p.get("citydata_area") for p in cfg["places"]}
    cands = hist.get("discovery", {}).get("candidates", {}).values()
    pick = [c for c in cands if len(c["hits"]) >= d.get("min_hits", 2) and not c.get("ev")
            and c["code"] not in fixed and c["name"] not in fixed]
    pick.sort(key=lambda c: -(c.get("max_ratio") or 0))
    prev = {p["id"]: p for p in hist.get("auto_places", [])}
    out = []
    for c in pick[:d.get("auto_track_max", 6)]:
        pid = "auto_" + c["code"]
        p = prev.get(pid) or {"id": pid, "label": c["name"], "citydata_area": c["code"], "air_urban": "",
                              "air_roadside": "", "auto": True, "kind": "population", "since": iso(now())}
        kw = [x.strip() for x in c["name"].replace("(", "·").replace(")", "").split("·") if len(x.strip()) >= 2][:3]
        p["keywords"] = kw or [c["name"]]
        out.append(p)
    hist["auto_places"] = out


# ---------------------------------------------------------------- 키워드(밈) 우선 발굴
# 순서: ① 밈 키워드 모으기 → ② 장소로 풀기 → ③ 게시물 급상승 점수 → ④ 추적 장소로 올리기
import html as _html
import hashlib
import re
from collections import Counter

STOP = set("""후기 리뷰 추천 방문 오늘 주말 평일 데이트 서울 카페 맛집 핫플 핫플레이스 팝업 팝업스토어 스토어 여행 코스 가볼만한곳
가볼만한 인생샷 사진 명소 웨이팅 오픈런 챌린지 성지순례 성지 브이로그 일상 정보 방법 가격 메뉴 위치 주차 시간 영업시간 예약 솔직
내돈내산 그리고 진짜 너무 완전 이번 다녀온 다녀왔어요 다녀왔다 요즘 뜨는 최근 이곳 여기 우리 같이 혼자 친구 가족 아이 추천해요
좋은 좋았던 예쁜 이쁜 분위기 감성 오픈 신상 근처 주변 모음 총정리 정리 리스트 베스트 BEST TOP 기록 이야기 하루 다녀옴 다녀온곳
그리고 하는 있는 없는 에서 으로 까지 부터 처럼 대한 위한 관련 소개 방문기 먹방 투어 나들이 산책 구경 체험 이벤트 굿즈 현장 할인
""".split())
MONTH = re.compile(r"^\d+(월|일|년|시|개|차|탄|편|번째|위|곳)?$")
PARTICLE = re.compile(r"(에서|으로|까지|부터|이랑|하고|에게|처럼|은|는|이|가|을|를|의|에|도|와|과|로)$")


def clean_title(t):
    return _html.unescape(re.sub(r"<[^>]+>", "", t or ""))


def terms_of(title, extra_stop):
    toks = []
    for w in re.findall(r"[가-힣A-Za-z0-9]+", clean_title(title)):
        if len(w) >= 3:
            w2 = PARTICLE.sub("", w)
            w = w2 if len(w2) >= 2 else w
        if 2 <= len(w) <= 12 and w not in STOP and w not in extra_stop and not MONTH.match(w):
            toks.append(w)
    out = set(toks)
    out.update(f"{a} {b}" for a, b in zip(toks, toks[1:]))   # 두 단어 묶음도 후보
    return out


def mine_rising_terms(cfg, keys, kw):
    """밈 성격 검색어로 최신 블로그·유튜브 제목을 모아, 평소보다 갑자기 많이 나오는 말을 찾는다.
    제목 자체는 저장하지 않고 단어별 개수만 남긴다."""
    k = cfg.get("keyword_discovery", {})
    extra = set(k.get("stopwords_extra", [])) | {w for q in k.get("seed_queries", []) for w in q.split()}
    counts = Counter()
    for q in k.get("seed_queries", []):
        _, items = naver_search(keys["NAVER_CLIENT_ID"], keys["NAVER_CLIENT_SECRET"], "blog", q, 1)
        for it in items:
            counts.update(terms_of(it.get("title"), extra))
        time.sleep(0.1)
    if keys.get("YOUTUBE_API_KEY") and k.get("mine_youtube", True):
        after = (now() - dt.timedelta(days=2)).astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for q in k.get("seed_queries", [])[:2]:     # 유튜브 할당량 절약: 앞 2개만
            try:
                j = json.loads(http("https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&order=date"
                                    f"&maxResults=50&regionCode=KR&relevanceLanguage=ko&publishedAfter={after}"
                                    f"&q={urllib.parse.quote(q)}&key={keys['YOUTUBE_API_KEY']}"))
                for it in j.get("items", []):
                    counts.update(terms_of(it.get("snippet", {}).get("title"), extra))
            except Exception:
                pass
    runs = kw.setdefault("runs", [])
    past_cut, recent_cut = iso(now() - dt.timedelta(days=8)), iso(now() - dt.timedelta(hours=20))
    base_runs = [r for r in runs if past_cut <= r["at"] < recent_cut]
    rising = []
    for term, c in counts.most_common(400):
        if c < k.get("min_count", 4):
            continue
        prev = sorted(r["counts"].get(term, 0) for r in base_runs)
        med = prev[len(prev) // 2] if prev else 0
        score = c / (med + 1)
        if not base_runs or score >= k.get("rise_threshold", 2.0):
            rising.append({"term": term, "count": c, "base": med, "rise": round(score, 2)})
    runs.append({"at": iso(now()), "counts": dict(counts.most_common(400))})
    kw["runs"] = runs[-60:]
    rising.sort(key=lambda r: (-r["rise"], -r["count"]))
    kw["rising"] = rising[:40]
    kw["baseline_runs"] = len(base_runs)
    return rising


def local_search(keys, term):
    url = f"https://openapi.naver.com/v1/search/local.json?display=5&query={urllib.parse.quote(term)}"
    j = json.loads(http(url, headers={"X-Naver-Client-Id": keys["NAVER_CLIENT_ID"],
                                      "X-Naver-Client-Secret": keys["NAVER_CLIENT_SECRET"]}))
    return j.get("items", [])


def citydata_names(hist):
    board = hist.get("discovery", {}).get("board", [])
    return [(b["code"], b["name"]) for b in board]


def match_citydata(text, names):
    """장소 이름·주소의 핵심 단어로 서울 주요 장소(도시데이터)와 잇는다."""
    best = None
    for code, name in names:
        parts = [p.strip() for p in re.split(r"[·()\s]", name) if len(p.strip()) >= 2]
        stems = {p for p in parts} | {re.sub(r"(역|공원|거리|시장|관광특구|한강공원|카페거리)$", "", p) for p in parts}
        stems = {s for s in stems if len(s) >= 2}
        hit = [s for s in stems if s in text]
        if hit and (best is None or len(max(hit, key=len)) > best[2]):
            best = (code, name, len(max(hit, key=len)))
    return {"code": best[0], "name": best[1], "_len": best[2]} if best else None


def resolve_place(keys, term, names):
    """키워드 → 장소. 서울 주요 장소 이름과 먼저 맞춰 보고, 없으면 네이버 지역 검색으로 찾는다."""
    cd = match_citydata(term, names)
    # 키워드가 거의 장소 이름 그 자체일 때만 바로 연결 (예: '서울숲' O, '망원 두바이쿠키' X → 지역 검색으로)
    if cd and cd.pop("_len", 0) < len(term.replace(" ", "")) * 0.6:
        cd = None
    if cd:
        return {"name": cd["name"], "citydata": cd, "gu": "", "addr": "", "category": "서울 주요 장소", "via": "주요 장소 이름"}
    items = [it for it in local_search(keys, term) if (it.get("address") or it.get("roadAddress") or "").startswith("서울")]
    if not items:
        return None
    it = items[0]
    addr = it.get("address") or it.get("roadAddress")
    parts = addr.split()
    gu = parts[1] if len(parts) > 1 and parts[1].endswith("구") else ""
    dong = re.sub(r"\d*(가|동)$", "", parts[2]) if len(parts) > 2 else ""
    lat = lon = None
    try:
        mx, my = int(it.get("mapx")), int(it.get("mapy"))
        if mx > 10 ** 8:                        # WGS84 × 10^7 형식
            lon, lat = mx / 1e7, my / 1e7
    except Exception:
        pass
    name = clean_title(it.get("title"))
    cd = match_citydata(name + " " + dong, names) if dong or name else None
    if cd:
        cd.pop("_len", None)
    return {"name": name, "citydata": cd, "gu": gu, "dong": dong, "addr": addr, "category": it.get("category", ""),
            "lat": lat, "lon": lon, "via": "네이버 지역 검색"}


def blog_rise(keys, term):
    """최근 3일 하루 평균 블로그 글 수 ÷ 그 전 7일 하루 평균."""
    _, items = naver_search(keys["NAVER_CLIENT_ID"], keys["NAVER_CLIENT_SECRET"], "blog", term, 1)
    today = now().date()
    ages = []
    for it in items:
        try:
            ages.append((today - dt.datetime.strptime(it["postdate"], "%Y%m%d").date()).days)
        except Exception:
            pass
    if not ages:
        return {"r3": 0, "rp": 0, "rise": 0}
    n3 = sum(1 for a in ages if a < 3)
    nprev = sum(1 for a in ages if 3 <= a < 10)
    full = len(ages) >= 100 and max(ages) < 3            # 100개가 모두 3일 안이면 매우 활발
    r3, rp = n3 / 3, nprev / 7
    rise = 10.0 if full else round(r3 / (rp + 0.5), 2)
    return {"r3": round(r3, 1), "rp": round(rp, 1), "rise": rise, "saturated": full}


def keyword_discovery(cfg, keys, hist):
    k = cfg.get("keyword_discovery", {})
    kw = hist.setdefault("keywords", {})
    names = citydata_names(hist)
    ev_re = re.compile("|".join(map(re.escape, k.get("event_words", []))) or "$^")
    cache = kw.setdefault("resolved", {})
    cands = kw.setdefault("cands", {})

    # ① 밈 키워드 모으기: 직접 넣은 키워드 + 블로그·유튜브 제목 급상승어 + 구글 급상승 검색어
    pinned_kw = {w for g in cfg["keyword_groups"] for w in g["keywords"]}   # 고정 장소 검색어는 발굴 대상에서 뺀다
    pool = [(t, "직접 입력") for t in k.get("seed_keywords", []) if t not in pinned_kw]
    pool += [(r["term"], "제목 급상승") for r in mine_rising_terms(cfg, keys, kw)[:k.get("rising_take", 20)]]
    pool += [(t["title"], "구글 급상승") for t in hist.get("trends", [])[:15]
             if t.get("last", "") >= iso(now() - dt.timedelta(hours=12))]
    pool = [(t, src) for t, src in pool if t not in pinned_kw]

    # ② 장소로 풀기 (7일 캐시) → ③ 점수
    resolved_now, budget = 0, k.get("resolve_max", 30)
    for term, src in pool:
        c = cache.get(term)
        if (not c or c["at"] < iso(now() - dt.timedelta(days=7))) and resolved_now < budget:
            try:
                c = {"at": iso(now()), "place": resolve_place(keys, term, names)}
            except Exception as e:
                log(f"장소 찾기 실패 '{term}': {e}")
                continue
            cache[term] = c
            resolved_now += 1
            time.sleep(0.1)
        if not c or not c.get("place"):
            continue
        pl = c["place"]
        e = cands.setdefault(term, {"term": term, "first": iso(now()), "hits": []})
        e["source"] = src if src == "직접 입력" or not e.get("source") else e["source"]
        e.update(place=pl, last_seen=iso(now()))
        try:
            e.update(blog_rise(keys, term))
        except Exception:
            pass
        e["ev"] = bool(ev_re.search(term + " " + pl.get("category", "") + " " + pl.get("name", "")))
        hot = e.get("rise", 0) >= k.get("score_threshold", 2.0) or src == "직접 입력"
        if hot:
            e["hits"] = [h for h in e["hits"] if h >= iso(now() - dt.timedelta(days=7))] + [iso(now())]
        time.sleep(0.1)
    # 14일 넘게 안 보인 후보 정리, 캐시도 30일 넘은 것 삭제
    kw["cands"] = {t: e for t, e in cands.items() if e.get("last_seen", "") >= iso(now() - dt.timedelta(days=14))}
    kw["resolved"] = {t: c for t, c in cache.items() if c["at"] >= iso(now() - dt.timedelta(days=30))}

    # 인구 급증 장소와 겹치면 '인구로 확인됨'
    surge = {c["code"] for c in hist.get("discovery", {}).get("candidates", {}).values() if not c.get("ev")}
    for e in kw["cands"].values():
        cd = (e["place"] or {}).get("citydata")
        e["pop_confirmed"] = bool(cd and cd["code"] in surge)

    # ④ 추적 장소로 올리기: 같은 장소로 풀린 키워드는 하나로 묶는다
    by_place = {}
    for e in kw["cands"].values():
        if e["ev"] or not e["hits"]:
            continue
        if e["source"] != "직접 입력" and len(e["hits"]) < k.get("min_hits", 2) and e.get("rise", 0) < k.get("instant_rise", 5):
            continue
        key = (e["place"].get("citydata") or {}).get("code") or e["place"]["name"]
        g = by_place.setdefault(key, {"place": e["place"], "terms": [], "score": 0, "seed": False, "pop": False})
        g["terms"].append(e["term"])
        g["score"] = max(g["score"], e.get("rise", 0))
        g["seed"] |= e["source"] == "직접 입력"
        g["pop"] |= e["pop_confirmed"]
    fixed = {p.get("citydata_area") for p in cfg["places"]} | {p["label"] for p in cfg["places"]}
    ranked = sorted(by_place.items(), key=lambda kv: (-kv[1]["seed"], -kv[1]["pop"], -kv[1]["score"]))
    out = []
    for key, g in ranked:
        pl = g["place"]
        cd = pl.get("citydata")
        if (cd and (cd["code"] in fixed or cd["name"] in fixed)) or pl["name"] in fixed:
            continue
        pid = "kw_" + hashlib.md5(key.encode()).hexdigest()[:8]
        out.append({"id": pid, "label": pl["name"], "kind": "keyword", "auto": True,
                    "citydata_area": cd["code"] if cd else "", "citydata_name": cd["name"] if cd else "",
                    "air_urban": pl.get("gu", ""), "air_roadside": "", "gu": pl.get("gu", ""),
                    "addr": pl.get("addr", ""), "lat": pl.get("lat"), "lon": pl.get("lon"),
                    "keywords": sorted(set(g["terms"]))[:5], "score": g["score"], "pop_confirmed": g["pop"],
                    "seed": g["seed"]})
        if len(out) >= k.get("auto_track_max", 8):
            break
    prev = {p["id"]: p.get("since") for p in hist.get("kw_places", [])}
    for p in out:
        p["since"] = prev.get(p["id"]) or iso(now())
    hist["kw_places"] = out
    kw["ranAt"] = iso(now())
    return len(out)


def all_places(cfg, hist):
    seen, out = set(), []
    for p in cfg["places"] + hist.get("kw_places", []) + hist.get("auto_places", []):
        key = p.get("citydata_area") or p["label"]
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def all_groups(cfg, hist):
    groups = list(cfg["keyword_groups"])
    names = {g["name"] for g in groups}
    for p in hist.get("kw_places", []) + hist.get("auto_places", []):
        if p["label"] not in names:
            groups.append({"name": p["label"], "place": p["id"], "keywords": p["keywords"], "auto": True})
            names.add(p["label"])
    return groups[:cfg.get("discovery", {}).get("max_groups", 15)]


# ---------------------------------------------------------------- 검색어 기반 게시물 수집 (공식 API만)
# 모든 SNS·지도 자료는 '개수·시각·집계값'만 저장한다. 작성자 이름, 계정, 본문, 사진은 저장하지 않는다.

def is_due(hist, src, hours):
    last = hist["status"].get(src, {}).get("lastSuccess")
    if not last:
        return True
    return now() - dt.datetime.strptime(last, "%Y-%m-%dT%H:%M").replace(tzinfo=KST) >= dt.timedelta(hours=hours) - dt.timedelta(minutes=10)


def merge_daily(old_rows, new_counts, covered_from):
    """covered_from 이후 날짜는 새 집계로 덮고, 그 이전은 기존 값을 유지한다."""
    merged = {d: v for d, v in (old_rows or []) if d < covered_from}
    merged.update({d: v for d, v in new_counts.items() if d >= covered_from})
    return sorted([[d, v] for d, v in merged.items()])


def naver_search(cid, secret, kind, query, pages):
    items, total = [], None
    for p in range(pages):
        url = (f"https://openapi.naver.com/v1/search/{kind}.json?display=100&sort=date&start={1 + p * 100}"
               f"&query={urllib.parse.quote(query)}")
        j = json.loads(http(url, headers={"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": secret}))
        total = j.get("total", total)
        got = j.get("items", [])
        items += got
        if len(got) < 100:
            break
        time.sleep(0.1)
    return total, items


def fetch_naver_buzz(cfg, keys, hist):
    """네이버 블로그·뉴스 검색 API: 검색어별 하루 게시물 수 (날짜순 최근 게시물 기준)."""
    soc = cfg.get("social", {})
    pages = soc.get("buzz_pages", 3)
    out = hist.get("buzz", {})
    for g in all_groups(cfg, hist):
        q = g.get("buzz_query") or g["keywords"][0]
        e = out.setdefault(g["name"], {})
        e.update(place=g["place"], query=q)
        for kind, datef in (("blog", lambda it: it.get("postdate", "")[:8]),
                            ("news", lambda it: dt.datetime.strptime(it["pubDate"], "%a, %d %b %Y %H:%M:%S %z")
                                                   .astimezone(KST).strftime("%Y%m%d"))):
            total, items = naver_search(keys["NAVER_CLIENT_ID"], keys["NAVER_CLIENT_SECRET"], kind, q, pages)
            counts = {}
            for it in items:
                try:
                    d8 = datef(it)
                    counts[f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}"] = counts.get(f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}", 0) + 1
                except Exception:
                    continue
            if not counts:
                continue
            # 가져온 개수가 한도에 닿았으면 가장 오래된 날은 일부만 잡힌 것이므로 제외
            days = sorted(counts)
            covered = days[1] if len(items) >= pages * 100 and len(days) > 1 else days[0]
            if len(items) >= pages * 100:
                counts.pop(days[0], None)
            e[kind + "_daily"] = merge_daily(e.get(kind + "_daily"), counts, covered)[-120:]
            e[kind + "_total"] = total
            e.setdefault(kind + "_total_hist", []).append([iso(now()), total])
            e[kind + "_total_hist"] = e[kind + "_total_hist"][-500:]
            time.sleep(0.1)
        e["fetchedAt"] = iso(now())
    hist["buzz"] = out
    return len(out)


def fetch_youtube(cfg, keys, hist):
    """YouTube Data API: 최근 7일 업로드 수와 그 영상들의 조회수 합 (날짜별)."""
    out = hist.get("youtube", {})
    after = (now() - dt.timedelta(days=7)).astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for g in all_groups(cfg, hist):
        q = g.get("buzz_query") or g["keywords"][0]
        url = ("https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&order=date&maxResults=50"
               f"&regionCode=KR&relevanceLanguage=ko&publishedAfter={after}&q={urllib.parse.quote(q)}"
               f"&key={keys['YOUTUBE_API_KEY']}")
        j = json.loads(http(url))
        vids = [(it["id"]["videoId"], it["snippet"]["publishedAt"]) for it in j.get("items", []) if it.get("id", {}).get("videoId")]
        views = {}
        if vids:
            v = json.loads(http("https://www.googleapis.com/youtube/v3/videos?part=statistics&id="
                                + ",".join(x[0] for x in vids) + f"&key={keys['YOUTUBE_API_KEY']}"))
            views = {it["id"]: int(it.get("statistics", {}).get("viewCount", 0)) for it in v.get("items", [])}
        cnt, vw = {}, {}
        for vid, pub in vids:
            d = dt.datetime.fromisoformat(pub.replace("Z", "+00:00")).astimezone(KST).strftime("%Y-%m-%d")
            cnt[d] = cnt.get(d, 0) + 1
            vw[d] = vw.get(d, 0) + views.get(vid, 0)
        e = out.setdefault(g["name"], {})
        covered = min(cnt) if len(vids) >= 50 and cnt else (now() - dt.timedelta(days=7)).strftime("%Y-%m-%d")
        if len(vids) >= 50 and cnt:
            cnt.pop(covered, None); vw.pop(covered, None)
            covered = min(cnt) if cnt else covered
        e.update(place=g["place"], query=q, fetchedAt=iso(now()), capped=len(vids) >= 50)
        e["daily"] = merge_daily(e.get("daily"), cnt, covered)[-120:]
        e["views"] = merge_daily(e.get("views"), vw, covered)[-120:]
    hist["youtube"] = out
    return len(out)


def fetch_instagram(cfg, keys, hist):
    """Instagram Graph API 해시태그 검색: 최근 24시간 공개 게시물의 시간별 개수와 반응 수 합.
    전문가(비즈니스/크리에이터) 계정과 Meta 앱이 필요하다. 7일 동안 서로 다른 해시태그 30개까지."""
    v = cfg.get("social", {}).get("graph_version", "v23.0")
    uid, tok = keys["IG_USER_ID"], keys["IG_ACCESS_TOKEN"]
    out = hist.get("instagram", {})
    errs = []
    for h in cfg.get("instagram_hashtags", []):
        tag = h["tag"].lstrip("#")
        e = out.setdefault(tag, {"rows": []})
        e["place"] = h.get("place")
        try:
            if not e.get("hid"):
                j = json.loads(http(f"https://graph.facebook.com/{v}/ig_hashtag_search?user_id={uid}"
                                    f"&q={urllib.parse.quote(tag)}&access_token={urllib.parse.quote(tok)}"))
                if not j.get("data"):
                    raise RuntimeError("해시태그를 찾지 못함")
                e["hid"] = j["data"][0]["id"]
            url = (f"https://graph.facebook.com/{v}/{e['hid']}/recent_media?user_id={uid}"
                   f"&fields=timestamp,like_count,comments_count&limit=50&access_token={urllib.parse.quote(tok)}")
            posts = []
            for _ in range(4):
                j = json.loads(http(url))
                posts += j.get("data", [])
                url = j.get("paging", {}).get("next")
                if not url:
                    break
            hours = {}
            for p in posts:
                t = dt.datetime.fromisoformat(p["timestamp"].replace("+0000", "+00:00")).astimezone(KST)
                k = t.strftime("%Y-%m-%dT%H:00")
                c = hours.setdefault(k, [0, 0])
                c[0] += 1
                c[1] += (p.get("like_count") or 0) + (p.get("comments_count") or 0)
            if hours:
                first = min(hours)
                hours.pop(first)          # 가장 오래된 시간대는 일부만 잡혔을 수 있음
                old = {r[0]: r[1:] for r in e["rows"] if r[0] < first}
                old.update(hours)
                e["rows"] = sorted([[k] + list(v2) for k, v2 in old.items()])[-24 * 60:]
            e["fetchedAt"] = iso(now())
            e["sample"] = len(posts)
        except Exception as ex:
            errs.append(f"#{tag}: {ex}")
    hist["instagram"] = out
    if errs:
        raise RuntimeError("; ".join(errs))
    return len(out)


def fetch_google_places(cfg, keys, hist):
    """Google Places API (New): 장소별 누적 리뷰 수·평점을 하루 한 번 기록 → 하루 새 리뷰 수로 방문 반응 추정."""
    key = keys["GOOGLE_MAPS_API_KEY"]
    out = hist.get("gplaces", {})
    for gp in cfg.get("google_places", []):
        e = out.setdefault(gp["query"], {"rows": []})
        e["place"] = gp.get("place")
        mask = "id,displayName,rating,userRatingCount"
        if not e.get("pid"):
            j = json.loads(http("https://places.googleapis.com/v1/places:searchText",
                                data=json.dumps({"textQuery": gp["query"], "languageCode": "ko", "regionCode": "KR"}).encode(),
                                headers={"Content-Type": "application/json", "X-Goog-Api-Key": key,
                                         "X-Goog-FieldMask": ",".join("places." + f for f in mask.split(","))}))
            if not j.get("places"):
                raise RuntimeError(f"'{gp['query']}' 장소를 찾지 못함")
            p = j["places"][0]
        else:
            p = json.loads(http(f"https://places.googleapis.com/v1/places/{e['pid']}",
                                headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": mask}))
        e["pid"] = p["id"]
        e["name"] = (p.get("displayName") or {}).get("text", gp["query"])
        today = now().strftime("%Y-%m-%d")
        e["rows"] = [r for r in e["rows"] if r[0] != today] + [[today, p.get("userRatingCount"), p.get("rating")]]
        e["rows"] = e["rows"][-400:]
    hist["gplaces"] = out
    return len(out)


def run_social(cfg, keys, hist):
    soc = cfg.get("social", {})
    jobs = [
        ("buzz", soc.get("buzz_refresh_hours", 3), keys.get("NAVER_CLIENT_ID") and keys.get("NAVER_CLIENT_SECRET"),
         fetch_naver_buzz, "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET (검색 API 추가 필요)"),
        ("youtube", soc.get("youtube_refresh_hours", 6), keys.get("YOUTUBE_API_KEY"), fetch_youtube, "YOUTUBE_API_KEY"),
        ("instagram", soc.get("instagram_refresh_hours", 3),
         keys.get("IG_USER_ID") and keys.get("IG_ACCESS_TOKEN") and cfg.get("instagram_hashtags"),
         fetch_instagram, "IG_USER_ID / IG_ACCESS_TOKEN"),
        ("gplaces", 23, keys.get("GOOGLE_MAPS_API_KEY") and cfg.get("google_places"),
         fetch_google_places, "GOOGLE_MAPS_API_KEY"),
    ]
    for src, hours, ok, fn, need in jobs:
        if not ok:
            if src in ("instagram", "gplaces", "youtube"):
                hist["status"].pop(src, None)   # 선택 항목: 키가 없으면 상태 표시도 하지 않음
            else:
                set_status(hist, src, False, f"인증키 없음 ({need})")
            continue
        if not is_due(hist, src, hours):
            continue
        try:
            n = fn(cfg, keys, hist)
            set_status(hist, src, True, "", n)
            log(f"{src} {n}개 갱신")
        except Exception as e:
            set_status(hist, src, False, str(e))
            log(f"{src} 오류: {e}")

# ---------------------------------------------------------------- 저장
def load_history():
    path = os.path.join(DATA_DIR, "history.json")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                d = json.load(f)
            if d.get("schema") == SCHEMA:
                return d
        except Exception:
            log("기존 history.json을 읽지 못해 새로 시작합니다.")
    return {"schema": SCHEMA, "obs": [], "search": {}, "x": {}, "status": {}, "runs": []}


def compact(obs, keep_days, full_days):
    """중복 제거(같은 자료원·장소·관측시각은 최신값), 오래된 자료는 시간당 1건으로 줄이고 보관기간 밖은 삭제."""
    cutoff = iso(now() - dt.timedelta(days=keep_days))
    thin = iso(now() - dt.timedelta(days=full_days))
    uniq = {}
    for r in obs:
        if r.get("t") and r["t"] >= cutoff:
            uniq[(r["src"], r["place"], r["t"])] = r
    out, seen_hour = [], set()
    for key in sorted(uniq):
        r = uniq[key]
        if r["t"] < thin:
            hk = (r["src"], r["place"], r["t"][:13])
            if hk in seen_hour:
                continue
            seen_hour.add(hk)
        out.append(r)
    return out


def save_json(name, obj):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = os.path.join(DATA_DIR, name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, os.path.join(DATA_DIR, name))


def save(hist):
    os.makedirs(DATA_DIR, exist_ok=True)
    body = json.dumps(hist, ensure_ascii=False, separators=(",", ":"))
    for name, content in (("history.json", body),
                          ("data.js", "window.__MEME_PULSE__=" + body + ";\n")):
        tmp = os.path.join(DATA_DIR, name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, os.path.join(DATA_DIR, name))


def set_status(hist, src, ok, msg="", n=0):
    st = hist["status"].setdefault(src, {})
    st["at"] = iso(now())
    st["ok"] = ok
    st["msg"] = msg[:300]
    st["n"] = n
    if ok:
        st["lastSuccess"] = st["at"]


# ---------------------------------------------------------------- 실행
def run_once(cfg, keys):
    hist = load_history()
    d = cfg.get("discovery", {})

    # 0. 자동 발굴 — 전체 장소 스캔 → 구글 트렌드 → 후보 자동 추적
    if d.get("enabled", True):
        if keys.get("SEOUL_API_KEY"):
            try:
                run_scan(cfg, keys["SEOUL_API_KEY"], hist)
            except Exception as e:
                set_status(hist, "scan", False, str(e))
                log(f"전체 장소 스캔 오류: {e}")
        else:
            set_status(hist, "scan", False, "인증키 없음 (SEOUL_API_KEY)")
        try:
            fetch_trends(hist, cfg)
            set_status(hist, "trends", True, "", len(hist.get("trends", [])))
        except Exception as e:
            set_status(hist, "trends", False, str(e))
            log(f"구글 트렌드 오류: {e}")
        auto_track(cfg, hist)

    # 0-1. 키워드(밈) 우선 발굴: 밈 키워드 → 장소 → 추적
    kd = cfg.get("keyword_discovery", {})
    if kd.get("enabled", True) and keys.get("NAVER_CLIENT_ID") and keys.get("NAVER_CLIENT_SECRET"):
        if is_due(hist, "kw", kd.get("every_hours", 3)):
            try:
                n = keyword_discovery(cfg, keys, hist)
                set_status(hist, "kw", True, "", n)
                log(f"키워드 발굴: 추적 장소 {n}곳")
            except Exception as e:
                set_status(hist, "kw", False, str(e))
                log(f"키워드 발굴 오류: {e}")
    elif kd.get("enabled", True):
        set_status(hist, "kw", False, "인증키 없음 (NAVER_CLIENT_ID / NAVER_CLIENT_SECRET, 검색 API 추가 필요)")

    places = all_places(cfg, hist)
    new = []

    # 서울 도시데이터
    if keys.get("SEOUL_API_KEY"):
        errs, n = [], 0
        for p in places:
            if not p.get("citydata_area"):
                continue
            try:
                recs = fetch_seoul(keys["SEOUL_API_KEY"], p)
                new += recs
                n += len(recs)
            except Exception as e:
                errs.append(str(e))
        set_status(hist, "seoul", n > 0, "; ".join(errs), n)
        log(f"서울 도시데이터 {n}건" + (f" / 오류: {'; '.join(errs)}" if errs else ""))
    else:
        set_status(hist, "seoul", False, "인증키 없음 (SEOUL_API_KEY)")

    # 에어코리아 (측정소별 한 번만 조회)
    if keys.get("AIRKOREA_API_KEY"):
        for kind, field in (("air_urban", "air_urban"), ("air_road", "air_roadside")):
            stations = {}
            for p in places:
                s = (p.get(field) or "").strip()
                if s:
                    stations.setdefault(s, []).append(p["id"])
            errs, n = [], 0
            for s, pids in stations.items():
                try:
                    recs = fetch_air(keys["AIRKOREA_API_KEY"], s, pids, kind)
                    new += recs
                    n += len(recs)
                except Exception as e:
                    errs.append(str(e))
                time.sleep(0.2)
            if stations:
                set_status(hist, kind, n > 0, "; ".join(errs), n)
                log(f"에어코리아 {kind} {n}건" + (f" / 오류: {'; '.join(errs)}" if errs else ""))
    else:
        set_status(hist, "air_urban", False, "인증키 없음 (AIRKOREA_API_KEY)")
        set_status(hist, "air_road", False, "인증키 없음 (AIRKOREA_API_KEY)")

    # 네이버 데이터랩 (일 단위 자료라 설정 간격마다만 갱신)
    if keys.get("NAVER_CLIENT_ID") and keys.get("NAVER_CLIENT_SECRET"):
        last = hist["status"].get("naver", {}).get("lastSuccess")
        tracked = set(hist.get("search", {}).keys())
        due = (not last or not hist.get("search") or {g["name"] for g in all_groups(cfg, hist)} - tracked
               or now() - dt.datetime.strptime(last, "%Y-%m-%dT%H:%M").replace(tzinfo=KST)
               >= dt.timedelta(hours=cfg.get("search_refresh_hours", 6)))
        if due:
            try:
                hist["search"] = fetch_naver(keys["NAVER_CLIENT_ID"], keys["NAVER_CLIENT_SECRET"],
                                             all_groups(cfg, hist), cfg.get("search_days", 90))
                set_status(hist, "naver", True, "", len(hist["search"]))
                log(f"네이버 검색 추이 {len(hist['search'])}개 그룹 갱신")
            except Exception as e:
                set_status(hist, "naver", False, str(e))
                log(f"네이버 오류: {e}")
        else:
            log("네이버 검색 추이: 갱신 주기 전이라 건너뜀")
    else:
        set_status(hist, "naver", False, "인증키 없음 (NAVER_CLIENT_ID / NAVER_CLIENT_SECRET)")

    # 검색어 기반 게시물: 네이버 블로그·뉴스, 유튜브, 인스타그램, 구글 지도 리뷰
    run_social(cfg, keys, hist)

    # X (선택)
    if keys.get("X_BEARER_TOKEN") and cfg.get("x_queries"):
        errs = []
        for q in cfg["x_queries"]:
            try:
                rows = fetch_x(keys["X_BEARER_TOKEN"], q)
                old = {r[0]: r[1] for r in hist["x"].get(q["name"], {}).get("rows", [])}
                old.update({r[0]: r[1] for r in rows})
                hist["x"][q["name"]] = {"place": q.get("place"), "query": q["query"],
                                        "rows": sorted([[k, v] for k, v in old.items()])[-24 * cfg.get("keep_days", 60):]}
            except Exception as e:
                errs.append(f"{q['name']}: {e}")
        set_status(hist, "x", not errs, "; ".join(errs), len(cfg["x_queries"]))
    else:
        hist["status"].pop("x", None)

    hist["obs"] = compact(hist["obs"] + new, cfg.get("keep_days", 60), cfg.get("full_resolution_days", 7))
    hist["places"] = places
    for p in places:
        p.setdefault("kind", "pinned")
    hist["keyword_groups"] = all_groups(cfg, hist)
    hist["generatedAt"] = iso(now())
    hist["runs"] = (hist.get("runs", []) + [{"at": hist["generatedAt"], "new": len(new)}])[-50:]
    hist["schema"] = SCHEMA
    save(hist)
    log(f"저장 완료: 누적 관측 {len(hist['obs'])}건 → data/data.js")


def check(cfg, keys):
    print("설정 점검")
    for k in ("SEOUL_API_KEY", "AIRKOREA_API_KEY", "NAVER_CLIENT_ID", "NAVER_CLIENT_SECRET", "X_BEARER_TOKEN",
              "YOUTUBE_API_KEY", "IG_USER_ID", "IG_ACCESS_TOKEN", "GOOGLE_MAPS_API_KEY"):
        v = keys.get(k)
        print(f"  {k:22s} {'있음 (' + v[:4] + '…)' if v else '없음'}")
    print(f"  장소 {len(cfg['places'])}곳, 검색어 그룹 {len(cfg['keyword_groups'])}개")
    for p in cfg["places"]:
        print(f"   - {p['label']}: 도시데이터 '{p.get('citydata_area')}', "
              f"도시대기 '{p.get('air_urban')}', 도로변 '{p.get('air_roadside') or '없음'}'")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", type=int, default=0, help="초 단위 반복 간격 (예: 600)")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--config", default=os.path.join(HERE, "config.json"))
    a = ap.parse_args()
    with open(a.config, encoding="utf-8") as f:
        cfg = json.load(f)
    keys = load_keys()
    if a.check:
        check(cfg, keys)
        return
    while True:
        try:
            run_once(cfg, keys)
        except Exception:
            traceback.print_exc()
        if not a.loop:
            break
        log(f"{a.loop}초 후 다시 수집합니다 (종료: Ctrl+C)")
        time.sleep(a.loop)


if __name__ == "__main__":
    sys.exit(main())
