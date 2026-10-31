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
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
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
              "IG_USER_ID", "IG_ACCESS_TOKEN", "GOOGLE_MAPS_API_KEY","KAKAO_REST_API_KEY"):
        if os.environ.get(k):
            keys[k] = os.environ[k].strip()
    return keys


# ---------------------------------------------------------------- 호출 수 보호 (하루 한도)
# 인증키마다 하루(구글 지도는 한 달) 호출 한도가 있다. 한도를 넘기면 그날 나머지 수집이 모두 실패해
# '지연'처럼 보이므로, 한도 근처에서 덜 중요한 호출부터 스스로 멈춘다.
API_OF_HOST = {
    "openapi.seoul.go.kr": "seoul", "apis.data.go.kr": "airkorea",
    "naverapihub.apigw.ntruss.com": "naver", "dapi.kakao.com": "kakao",
    "www.googleapis.com": "youtube", "places.googleapis.com": "gplaces",
    "graph.facebook.com": "instagram", "api.x.com": "x", "trends.google.com": "trends",
}
DEFAULT_LIMITS = {"airkorea": 480, "naver": 24000, "kakao": 90000, "youtube": 9000}   # 하루 기준
MONTHLY = {"gplaces"}                                                                  # 한 달 기준
_QUOTA = {"hist": None, "limits": {}, "lock": threading.Lock()}


def quota_key(api):
    return now().strftime("%Y-%m") if api in MONTHLY else now().strftime("%Y-%m-%d")


def quota_init(hist, cfg):
    q = hist.setdefault("quota", {})
    keep = {now().strftime("%Y-%m-%d"), (now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"), now().strftime("%Y-%m")}
    for api in list(q):
        q[api] = {k: v for k, v in q[api].items() if k in keep}
    lim = dict(DEFAULT_LIMITS)
    lim.update(cfg.get("api_limits", {}))
    lim.setdefault("gplaces", (cfg.get("google_maps") or {}).get("monthly_cap", 900))
    _QUOTA.update(hist=hist, limits={k: v for k, v in lim.items() if not str(k).startswith("_")})


def quota_count(api):
    h = _QUOTA["hist"]
    if h is None or not api:
        return 0
    return h.setdefault("quota", {}).get(api, {}).get(quota_key(api), 0)


def quota_take(api):
    h = _QUOTA["hist"]
    if h is None or not api:
        return
    with _QUOTA["lock"]:
        lim = _QUOTA["limits"].get(api)
        used = quota_count(api)
        if lim and used >= lim:
            raise RuntimeError(f"호출 한도 보호: {api} {'이번 달' if api in MONTHLY else '오늘'} {used}/{lim}회 사용 (config.json api_limits)")
        h["quota"].setdefault(api, {})[quota_key(api)] = used + 1


def http(url, data=None, headers=None, timeout=25, retries=2):
    """일시적 오류(접속 끊김, 시간 초과, 5xx, 429)는 잠깐 쉬었다가 다시 시도한다.
    인증·요청 오류(4xx)는 다시 해도 같으므로 바로 알린다."""
    api = API_OF_HOST.get(urllib.parse.urlsplit(url).hostname or "")
    for attempt in range(retries + 1):
        quota_take(api)
        req = urllib.request.Request(url, data=data, headers=headers or {})
        req.add_header("User-Agent", "MemePulse/1.1")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")[:300]
            if (e.code >= 500 or e.code == 429) and attempt < retries:
                time.sleep(2 * (attempt + 1) + (3 if e.code == 429 else 0))
                continue
            raise RuntimeError(f"HTTP {e.code}: {body}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"접속 실패: {getattr(e, 'reason', e)}") from None


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
           f"?serviceKey={k}&returnType=json&numOfRows=24&pageNo=1"   # 24시간치: 수집이 몇 시간 늦어도 빈칸 없이 메움
           f"&stationName={urllib.parse.quote(station)}&dataTerm=DAILY&ver=1.3")
    txt = http(url, timeout=30, retries=2)
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
                "keywordGroups": [{"groupName": g["name"], "keywords": g["keywords"][:5]} for g in chunk],
            }
            if ages:
                body["ages"] = ages
            txt = http("https://naverapihub.apigw.ntruss.com/search-trend/v1/search",
                       data=json.dumps(body).encode("utf-8"),
                       headers={"X-NCP-APIGW-API-KEY-ID": cid, "X-NCP-APIGW-API-KEY": secret,
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
    wt = first_dict_with(city.get("WEATHER_STTS"), "WEATHER_TIME") or {}
    road = find(city.get("ROAD_TRAFFIC_STTS"), "AVG_ROAD_DATA") or {}
    return {"code": code, "name": city.get("AREA_NM") or pp.get("AREA_NM") or code,
            "t": parse_time(pp.get("PPLTN_TIME")) or iso(now()),
            "pop": (lo + hi) / 2, "cg": CONGEST.get(str(pp.get("AREA_CONGEST_LVL", "")).strip()),
            "y": y, "ev": event,
            # 아래 값은 추천 계산용(현재 상태)으로만 쓰고 scan.json에는 저장하지 않는다
            "_spd": num(road.get("ROAD_TRAFFIC_SPD")), "_wind": num(wt.get("WIND_SPD")),
            "_pm25": num(wt.get("PM25")), "_pm10": num(wt.get("PM10")), "_temp": num(wt.get("TEMP")),
            "_rain": num(wt.get("PRECIPITATION")) if str(wt.get("PRECIPITATION", "")).replace(".", "", 1).isdigit() else None}


def load_json(name, default):
    path = os.path.join(DATA_DIR, name)
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _slot_hw(t):
    x = dt.datetime.strptime(t[:16], "%Y-%m-%dT%H:%M")
    return x.hour, x.weekday() >= 5


def baseline_of(rows_of_code, t, min_n=3):
    """같은 장소, 같은 평일/주말, 앞뒤 1시간 이내 시각, 최근 24시간 제외한 인구 중앙값."""
    import statistics
    recent = iso(now() - dt.timedelta(hours=24))
    h, we = _slot_hw(t)
    base = [b for b in rows_of_code if b["t"] < recent and _slot_hw(b["t"])[1] == we
            and min(abs(_slot_hw(b["t"])[0] - h), 24 - abs(_slot_hw(b["t"])[0] - h)) <= 1]
    if len(base) < min_n:
        return None, None, len(base)
    return (statistics.median([b["pop"] for b in base]),
            statistics.median([b.get("y") or 0 for b in base]), len(base))


def pmap(fn, items, workers=4):
    """작은 스레드 묶음으로 동시에 호출한다 (서울·에어코리아 응답 대기 시간을 줄임)."""
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        return list(ex.map(fn, items))


def run_scan(cfg, key, hist):
    """주요 장소 전체를 훑어 평소 같은 시간대 대비 인구 배율을 계산한다."""
    d = cfg.get("discovery", {})
    scan = load_json("scan.json", {"rows": [], "codes": []})
    codes = scan.get("codes") or [f"POI{i:03d}" for i in range(1, d.get("max_code", 130) + 1)]

    def one(code):
        try:
            return scan_place(key, code), None
        except Exception as e:
            return None, str(e)
    res = pmap(one, codes, (cfg.get("schedule") or {}).get("_workers", 4))
    rows = [r for r, _ in res if r]
    found = [r["code"] for r in rows]
    errs = sum(1 for _, e in res if e)
    if not rows:
        first_err = next((e for _, e in res if e), "")
        raise RuntimeError(f"응답한 장소가 없습니다 (오류 {errs}건) {first_err[:120]}")
    if not scan.get("codes"):
        scan["codes"] = found            # 처음 한 번 실제 존재하는 코드만 기억
    cutoff = iso(now() - dt.timedelta(days=d.get("baseline_days", 28)))
    stored = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    scan["rows"] = [r for r in scan.get("rows", []) if r["t"] >= cutoff] + stored
    save_json("scan.json", scan)
    hist["_scan_live"] = {r["code"]: r for r in rows}     # 이번 실행에서만 쓰는 현재 상태 (저장 안 함)

    by_code = {}
    for r in scan["rows"]:
        by_code.setdefault(r["code"], []).append(r)
    cands = hist.get("discovery", {}).get("candidates", {})
    board = []
    for r in rows:
        med, ymed, n = baseline_of(by_code.get(r["code"], []), r["t"], d.get("min_baseline", 3))
        ratio = r["pop"] / med if med else None
        item = {"code": r["code"], "name": r["name"], "t": r["t"], "pop": r["pop"], "cg": r["cg"],
                "ratio": round(ratio, 2) if ratio else None, "base_n": n,
                "dy": round(r["y"] - ymed, 1) if ymed is not None else None, "ev": r["ev"],
                "spd": r.get("_spd"), "wind": r.get("_wind"), "pm25": r.get("_pm25")}
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
    log(f"전체 장소 스캔 {len(rows)}곳, 후보 {len(cands)}곳" + (f", 오류 {errs}건" if errs else ""))
    return len(rows)


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


# ---------------------------------------------------------------- 밈 키워드 품질 검사
# 실제 수집 기록에서 '운전'→운전학원, '조국'→호프집, '가을'→카페 '가을' 처럼 흔한 말이 엉뚱한 가게로
# 풀려 추적 장소를 오염시켰다. 장소 밈으로 볼 수 있는 키워드만 통과시킨다.
AREA_STEMS = set("""성수 성수동 연남 연남동 연희 망원 망리단길 합정 상수 홍대 신촌 이대 을지로 힙지로 익선 익선동 종로 북촌 서촌
삼청 인사동 명동 남산 이태원 한남 해방촌 경리단길 용리단길 용산 삼각지 압구정 로데오 청담 신사 가로수길 강남 역삼 선릉 삼성
코엑스 잠실 석촌 석촌호수 송리단길 방이 건대 뚝섬 왕십리 서울숲 여의도 더현대 영등포 문래 당산 목동 마곡 상암 난지 하늘공원
노들섬 노들 반포 서래 세빛섬 고속터미널 동대문 DDP 혜화 대학로 광장시장 낙산 청계천 광화문 시청 덕수궁 경복궁 창덕궁 창경궁
롯데월드 롯데타워 한강 북한산 인왕산 수유 공릉 경의선숲길 연트럴파크 샤로수길 신림 사당 노량진 흑석 대림 가산 신도림 천호
고덕 서대문 은평 불광 연신내 정릉 쌍문 창동 청량리 회기 동묘 신당 신당동 약수 후암 해방촌 남영 서울역 북서울꿈의숲 올림픽공원
여의도공원 선유도 양화 망원한강 반포한강 이촌 뚝섬한강 잠원 응봉 낙성대 보라매 서울대공원 어린이대공원 남산타워""".split())
SEASON_MEME = set("""핑크뮬리 억새 억새축제 단풍길 벚꽃 벚꽃길 코스모스 수국 튤립 해바라기 은행나무 은행나무길 메타세쿼이아 야경 루미나리
일루미네이션 크리스마스트리 빛축제 불꽃놀이 장미 장미원 유채꽃 국화 국화축제 설경""".split())
GENERIC = set("""가을 겨울 봄 여름 일정 이유 비교 많은 필수 완벽 이제 꿀팁 솔직후기 본점 디저트 술집 단풍 단풍명소 코스 빵지순례 맛집투어
역대급 놓치면 온라인 부모님 오션뷰 할로윈 크리스마스 주차 가격 후기 데이트코스 데이트 여행 혼밥 브런치 고기 고기집 고깃집 편의점
쇼핑 대여 러닝 귀여운 듀오 가수 과일 어린이집 저작권 일상 근황 리뷰 추천 정보 방법 신상 오픈 이벤트 할인 굿즈 체험 나들이""".split())
CAT_BLOCK = ("의료", "병원", "약국", "미용", "부동산", "교육", "학원", "학교", "사회,공공기관", "공공기관", "종교", "교회", "성당",
             "교통,수송", "서비스,산업", "금융", "은행", "주거", "회사", "기업", "대사관", "법률", "주유", "자동차", "숙박", "산업")
CAT_FRIENDLY = ("카페", "디저트", "제과", "베이커리", "팝업", "이벤트", "전시", "미술관", "박물관", "갤러리", "공원", "관광", "명소",
                "쇼핑", "백화점", "테마", "거리", "시장", "음식점", "서울 주요 장소", "문화,예술", "여행")
CAT_OUTDOOR = ("공원", "거리", "관광", "명소", "시장", "서울 주요 장소", "광장", "한강", "숲")


def _ns(s):
    return re.sub(r"\s+", "", s or "")


def place_stems(place, names=()):
    """장소에서 '지역 이름'으로 쓸 수 있는 말들 (구·동, 서울 주요 장소 이름 줄기)."""
    st = set(AREA_STEMS)
    if place:
        gu = (place.get("gu") or "").rstrip("구")
        if len(gu) >= 2:
            st.add(gu)
        if place.get("dong") and len(place["dong"]) >= 2:
            st.add(place["dong"])
        cd = place.get("citydata") or {}
        for p in re.split(r"[·()\s]", cd.get("name", "")):
            p = re.sub(r"(역|관광특구|한강공원|카페거리|거리|공원|시장)$", "", p.strip())
            if len(p) >= 2:
                st.add(p)
    return {s for s in st if len(s) >= 2}


def meme_gate(term, place, source, trend_related=True, names=()):
    """(통과 여부, 이유, 종류) — 종류: '장소 밈' | '지역명' | '계절 밈' | '가게 밈' | '직접 입력'"""
    if source == "직접 입력":
        return True, "", "직접 입력"
    if not place:
        return False, "서울 장소로 풀리지 않음", ""
    cat = place.get("category", "")
    if any(b in cat for b in CAT_BLOCK):
        return False, "장소 종류가 밈과 무관 (병원·학원·회사·은행 등)", ""
    if source == "구글 급상승" and not trend_related:
        return False, "뉴스·인물 급상승어 (장소 관련 단어 없음)", ""
    t = _ns(term)
    if term in GENERIC or t in GENERIC:
        return False, "흔한 말", ""
    if re.fullmatch(r"[\d,.]+(원|천원|만원|%|개|명|층|호|번)?", t):
        return False, "숫자·가격", ""
    toks = term.split()
    stems = place_stems(place, names)
    area = [s for s in stems if any(tok.startswith(s) for tok in toks)]
    if area:
        only_area = all(any(tok == s or tok == s + "동" for s in stems) for tok in toks)
        return True, "", "지역명" if only_area else "장소 밈"
    season = any(any(tok.startswith(w) for w in SEASON_MEME) for tok in toks)
    if season:
        if place.get("citydata") or any(c in cat for c in CAT_OUTDOOR):
            return True, "", "계절 밈"
        return False, "계절 밈이지만 야외 명소로 풀리지 않음", ""
    name = _ns(place.get("name", ""))
    name_hit = len(t) >= 3 and (t in name or any(len(w) >= 3 and w in t for w in re.split(r"\s+", place.get("name", ""))))
    if name_hit and any(c in cat for c in CAT_FRIENDLY):
        amb = place.get("amb")
        if amb is not None and amb >= 3:
            return False, f"같은 이름의 가게가 서울에 {amb}곳 (특정 불가)", ""
        if len(toks) >= 2 or len(t) >= 4 or amb is not None:
            return True, "", "가게 밈"
    return False, "장소를 특정하기 어려운 말", ""


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
    yt_day = now().strftime("%Y-%m-%d")
    if keys.get("YOUTUBE_API_KEY") and k.get("mine_youtube", True) and kw.get("yt_mined") != yt_day:
        kw["yt_mined"] = yt_day          # 유튜브 검색은 1회 100단위(하루 약 100회 한도) → 제목 채굴은 하루 한 번만
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

def kakao_local_search(keys, term, size=5):
    url = ("https://dapi.kakao.com/v2/local/search/keyword.json?"
           + urllib.parse.urlencode({"query": term, "size": size}))
    headers = {"Authorization": f"KakaoAK {keys['KAKAO_REST_API_KEY']}"}
    j = json.loads(http(url, headers=headers))
    return j.get("documents", [])

def fetch_kakao(cfg, keys, hist):
    """Kakao 장소 검색 결과를 history에 저장."""
    out = hist.get("kakao", {})
    total = 0

    # config에 kakao_places가 있으면 그것을 사용
    queries = cfg.get("kakao_places", [])

    # 없으면 google_places의 query를 재사용
    if not queries:
        queries = [
            {"query": x.get("query"), "place": x.get("place")}
            for x in cfg.get("google_places", [])
            if x.get("query")
        ]

    for item in queries:
        query = item.get("query")
        if not query:
            continue

        results = kakao_local_search(keys, query)

        out[query] = {
            "query": query,
            "place": item.get("place", query),
            "fetchedAt": iso(now()),
            "results": results,
        }

        total += len(results)

    hist["kakao"] = out
    return total

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
        # 낱말 앞부분이 맞을 때만 인정 ('봉명동'이 '명동'으로 잘못 이어지던 문제)
        words = re.split(r"\s+", text)
        hit = [s for s in stems if any(w.startswith(s) for w in words)]
        if hit and (best is None or len(max(hit, key=len)) > best[2]):
            best = (code, name, len(max(hit, key=len)))
    return {"code": best[0], "name": best[1], "_len": best[2]} if best else None


def resolve_place(keys, term, names):
    """키워드 → 장소. 서울 주요 장소 이름과 먼저 맞춰 보고, 없으면 네이버 지역 검색으로 찾는다."""
    cd = match_citydata(term, names)
    partial = None
    # 키워드가 거의 장소 이름 그 자체일 때만 바로 연결 (예: '서울숲' O, '망원 두바이쿠키' X → 지역 검색으로)
    if cd and cd.pop("_len", 0) < len(term.replace(" ", "")) * 0.6:
        partial, cd = cd, None
    if cd:
        return {"name": cd["name"], "citydata": cd, "gu": "", "addr": "", "category": "서울 주요 장소", "via": "주요 장소 이름"}
    raw = kakao_local_search(keys, term, size=15)
    items = [it for it in raw if (it.get("address_name") or "").startswith("서울")]
    if not items:
        # 지역 검색에 없는 행사·현상 이름(예: '석촌호수 루미나리')도 주요 장소 이름이 들어 있으면 그 장소로 잇는다
        if partial:
            return {"name": partial["name"], "citydata": partial, "gu": "", "addr": "", "category": "서울 주요 장소",
                    "via": "주요 장소 이름(일부)"}
        return None
    it = items[0]
    # 같은 말을 이름에 가진 서로 다른 서울 가게 수 (많으면 '특정 불가')
    t0 = _ns(term)
    amb = len({_ns(x.get("place_name")) for x in items if t0 and t0 in _ns(x.get("place_name"))})
    full = it.get("address_name") or ""
    addr = it.get("road_address_name") or full
    parts = full.split()
    gu = parts[1] if len(parts) > 1 and parts[1].endswith("구") else ""
    dong = re.sub(r"\d*(가|동)$", "", parts[2]) if len(parts) > 2 else ""
    try:
        lon, lat = float(it["x"]), float(it["y"])
    except Exception:
        lon = lat = None
    name = it.get("place_name") or term
    cd = match_citydata(name + " " + dong, names)
    if cd:
        cd.pop("_len", None)
    return {"name": name, "citydata": cd, "gu": gu, "dong": dong, "addr": addr,
            "category": it.get("category_name", ""), "lat": lat, "lon": lon,
            "url": it.get("place_url", ""), "amb": amb,
            "via": "카카오 로컬 검색"}

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


def roadside_for(cfg, hist, gu):
    """구에 짝지을 도로변 측정소. 실제 서울 측정소 목록에 없는 이름이면 쓰지 않는다."""
    rs = (cfg.get("air_roadside_by_gu") or {}).get(gu or "", "")
    if not rs or rs in hist.get("station_bad", {}):
        return ""
    known = hist.get("seoul_stations")
    if known and rs not in known:
        return ""
    return rs


def gu_of(pl, geo):
    if not pl:
        return ""
    return pl.get("gu") or (geo.get(((pl.get("citydata") or {}).get("code")) or "", {}) or {}).get("gu", "")


def ensure_poi_geo(keys, hist, limit=40):
    """서울 주요 장소(POI)마다 구·좌표를 한 번만 찾아 둔다 (카카오). 측정소 짝짓기와 지도 표시에 쓴다."""
    geo = hist.setdefault("poi_geo", {})
    if not keys.get("KAKAO_REST_API_KEY"):
        return 0
    todo = [(c, n) for c, n in citydata_names(hist) if c not in geo][:limit]
    for code, name in todo:
        q = re.split(r"[·(]", name)[0].strip()
        try:
            items = [it for it in kakao_local_search(keys, q, size=5) if (it.get("address_name") or "").startswith("서울")]
        except Exception:
            continue
        if not items:
            geo[code] = {"name": name, "gu": "", "lat": None, "lon": None}
            continue
        it = items[0]
        parts = (it.get("address_name") or "").split()
        geo[code] = {"name": name, "gu": parts[1] if len(parts) > 1 and parts[1].endswith("구") else "",
                     "lat": float(it["y"]), "lon": float(it["x"]), "addr": it.get("road_address_name") or it.get("address_name")}
        time.sleep(0.05)
    return len(todo)


def keyword_discovery(cfg, keys, hist):
    k = cfg.get("keyword_discovery", {})
    kw = hist.setdefault("keywords", {})
    names = citydata_names(hist)
    ensure_poi_geo(keys, hist)
    geo = hist.get("poi_geo", {})
    ev_re = re.compile("|".join(map(re.escape, k.get("event_words", []))) or "$^")
    cache = kw.setdefault("resolved", {})
    cands = kw.setdefault("cands", {})

    # ① 밈 키워드 모으기: 직접 넣은 키워드 + 블로그·유튜브 제목 급상승어 + 구글 급상승 검색어
    pinned_kw = {w for g in cfg["keyword_groups"] for w in g["keywords"]}   # 고정 장소 검색어는 발굴 대상에서 뺀다
    pool = [(t, "직접 입력") for t in k.get("seed_keywords", []) if t not in pinned_kw]
    pool += [(r["term"], "제목 급상승") for r in mine_rising_terms(cfg, keys, kw)[:k.get("rising_take", 20)]]
    # 구글 급상승어는 대부분 뉴스·인물이다. 장소 이름이나 장소 관련 단어가 함께 나온 것만 쓴다.
    pool += [(t["title"], "구글 급상승") for t in hist.get("trends", [])[:30]
             if t.get("last", "") >= iso(now() - dt.timedelta(hours=12)) and (t.get("places") or t.get("words"))]
    pool = [(t, src) for t, src in pool if t not in pinned_kw]
    # 흔한 말은 지역 검색 호출 자체를 하지 않는다
    pool = [(t, src) for t, src in pool if src == "직접 입력" or (t not in GENERIC and _ns(t) not in GENERIC)]

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
        ok, why, kind = meme_gate(term, pl, e["source"], True, names)
        e["gate"] = {"ok": ok, "why": why, "kind": kind}
        if not ok:
            e["hits"] = []
            continue                                  # 블로그 호출도 아낀다
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
        if "gate" not in e:          # 이전 버전에서 만든 후보도 같은 기준으로 다시 검사
            ok, why, kind = meme_gate(e["term"], e.get("place"), e.get("source", ""), True, names)
            e["gate"] = {"ok": ok, "why": why, "kind": kind}
        if not e["gate"]["ok"] or e.get("ev") or not e["hits"]:
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
                    "air_urban": gu_of(pl, geo), "gu": gu_of(pl, geo),
                    "air_roadside": roadside_for(cfg, hist, gu_of(pl, geo)) if k.get("auto_roadside", True) else "",
                    "addr": pl.get("addr", "") or (geo.get((cd or {}).get("code"), {}) or {}).get("addr", ""),
                    "lat": pl.get("lat") or (geo.get((cd or {}).get("code"), {}) or {}).get("lat"),
                    "lon": pl.get("lon") or (geo.get((cd or {}).get("code"), {}) or {}).get("lon"),
                    "url": pl.get("url", ""), "category": pl.get("category", ""),
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


def naver_search(cid, secret, kind, query, pages, stop_before=None, datef=None):
    """stop_before(YYYYMMDD)가 주어지면, 받은 글이 그 날짜보다 오래된 것까지 닿는 순간 다음 쪽을 부르지 않는다
    (이미 모아 둔 날은 다시 받을 필요가 없음 → 호출 수 절약)."""
    items, total = [], None
    for p in range(pages):
        url = (f"https://naverapihub.apigw.ntruss.com/search/v1/{kind}?display=100&sort=date&start={1 + p * 100}"
               f"&query={urllib.parse.quote(query)}")
        j = json.loads(http(url, headers={"X-NCP-APIGW-API-KEY-ID": cid, "X-NCP-APIGW-API-KEY": secret}))
        total = j.get("total", total)
        got = j.get("items", [])
        items += got
        if len(got) < 100:
            break
        if stop_before and datef:
            try:
                if min(datef(it) for it in got) < stop_before:
                    break
            except Exception:
                pass
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
            have = [d for d, _ in (e.get(kind + "_daily") or [])]
            stop = have[-2].replace("-", "") if len(have) >= 2 else None      # 마지막으로 완전히 모은 날까지만
            total, items = naver_search(keys["NAVER_CLIENT_ID"], keys["NAVER_CLIENT_SECRET"], kind, q, pages, stop, datef)
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
            partial = len(items) >= 100 and len(items) % 100 == 0      # 쪽을 끝까지 받지 않았으면 가장 오래된 날은 일부만
            covered = days[1] if partial and len(days) > 1 else days[0]
            if partial:
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


def google_targets(cfg, hist):
    """구글 지도로 볼 장소: config의 google_places + (자동) 추적 중인 밈 장소 + 추천 상위 장소."""
    gm = cfg.get("google_maps") or {}
    out = [dict(q, auto=False) for q in cfg.get("google_places", [])]
    if not gm.get("auto_places", True):
        return out
    seen = {q.get("place") for q in out}
    auto = []
    for p in hist.get("kw_places", []):
        auto.append({"query": f"{p['label']} {p.get('gu', '')}".strip(), "place": p["id"],
                     "lat": p.get("lat"), "lon": p.get("lon")})
    for r in (hist.get("recs") or {}).get("items", [])[:gm.get("auto_recs", 5)]:
        auto.append({"query": f"{r['label']} {r.get('gu', '')}".strip(), "place": r.get("tracked_id") or "rec:" + r["key"],
                     "lat": r.get("lat"), "lon": r.get("lon")})
    for q in auto:
        if q["place"] in seen:
            continue
        seen.add(q["place"])
        out.append(dict(q, auto=True))
        if sum(1 for x in out if x["auto"]) >= gm.get("max_auto_places", 12):
            break
    return out


def fetch_google_places(cfg, keys, hist):
    """Google Places API (New): 장소별 누적 리뷰 수·평점을 하루 한 번 기록 → 하루 새 리뷰 수로 방문 반응 추정.
    rating·userRatingCount 필드 때문에 Enterprise 요금 단위(월 1,000회 무료)로 계산된다. 장소 ID는 저장해 두고
    다음부터는 Place Details 한 번만 부른다. 월 호출 수는 api_limits / google_maps.monthly_cap 으로 막는다."""
    key = keys["GOOGLE_MAPS_API_KEY"]
    out = hist.get("gplaces", {})
    mask = "id,displayName,rating,userRatingCount,location,googleMapsUri"
    errs, n = [], 0
    for gp in google_targets(cfg, hist):
        e = out.setdefault(gp["query"], {"rows": []})
        e["place"] = gp.get("place")
        e["auto"] = gp.get("auto", False)
        try:
            if not e.get("pid"):
                body = {"textQuery": gp["query"], "languageCode": "ko", "regionCode": "KR"}
                if gp.get("lat") and gp.get("lon"):   # 카카오 좌표 주변 300 m로 좁혀 동명이소를 피함
                    body["locationBias"] = {"circle": {"center": {"latitude": gp["lat"], "longitude": gp["lon"]}, "radius": 300.0}}
                j = json.loads(http("https://places.googleapis.com/v1/places:searchText", data=json.dumps(body).encode(),
                                    headers={"Content-Type": "application/json", "X-Goog-Api-Key": key,
                                             "X-Goog-FieldMask": ",".join("places." + f for f in mask.split(","))}))
                if not j.get("places"):
                    raise RuntimeError(f"'{gp['query']}' 장소를 찾지 못함")
                p = j["places"][0]
            else:
                p = json.loads(http(f"https://places.googleapis.com/v1/places/{e['pid']}",
                                    headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": mask}))
        except Exception as ex:
            errs.append(str(ex)[:160])
            if "호출 한도 보호" in str(ex):
                break
            continue
        e["pid"] = p["id"]
        e["name"] = (p.get("displayName") or {}).get("text", gp["query"])
        loc = p.get("location") or {}
        e.update(lat=loc.get("latitude"), lon=loc.get("longitude"), uri=p.get("googleMapsUri"),
                 rating=p.get("rating"), reviews=p.get("userRatingCount"))
        today = now().strftime("%Y-%m-%d")
        e["rows"] = [r for r in e["rows"] if r[0] != today] + [[today, p.get("userRatingCount"), p.get("rating")]]
        e["rows"] = e["rows"][-400:]
        n += 1
    # 30일 넘게 갱신되지 않은 자동 항목은 정리
    old = (now() - dt.timedelta(days=30)).strftime("%Y-%m-%d")
    hist["gplaces"] = {k: v for k, v in out.items() if not v.get("auto") or (v.get("rows") and v["rows"][-1][0] >= old)}
    if errs and not n:
        raise RuntimeError("; ".join(errs[:3]))
    return n


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


# ---------------------------------------------------------------- 엄격한 갱신 주기 (자료원별 시간표)
# 예전에는 '마지막 성공 + n시간'으로 판단해서, 한 번 늦으면 그 뒤 일정이 계속 밀렸다(드리프트).
# 이제는 하루를 자료원마다 정해진 칸(slot)으로 나누고, 지금 칸에서 아직 성공하지 못한 자료원만 부른다.
# 예) 에어코리아: 60분 칸, 매시 20분 시작 → 10:20~11:19 사이 한 번, 11:20부터 다음 칸.
# 실패하면 같은 칸 안에서 몇 분 뒤 다시 시도한다. 늦게 실행돼도 다음 칸은 원래 시각에 맞춰 돌아온다.
SCHED_DEFAULT = {            # (주기 분, 시작 오프셋 분)  — 모두 한국시간 기준
    "seoul": (10, 0),        # 도시데이터: 5분마다 갱신, 과거 값은 다시 받을 수 없으므로 가장 촘촘하게
    "recs_live": (10, 0),    # 추천 상위 장소의 실시간 인구·바람
    "air": (60, 20),         # 에어코리아: 매시 정각 값이 15~20분 뒤 올라옴. 24시간치를 받아 빈칸을 메움
    "trends": (60, 2),
    "x": (60, 8),
    "scan": (180, 5),
    "kw": (180, 10),
    "buzz": (180, 15),
    "instagram": (180, 25),
    "naver": (360, 30),
    "youtube": (360, 40),
    "kakao": (1440, 60),
    "gplaces": (1440, 1425),  # 하루 한 번 23:45 → 날짜별 누적 리뷰 수가 같은 시각 기준이 됨
}
STATUS_IDS = {"air": ("air_urban", "air_road")}


def cadence(cfg, src):
    s = (cfg.get("schedule") or {}).get(src)
    if isinstance(s, dict) and s.get("every_min"):
        return int(s["every_min"]), int(s.get("offset_min", 0))
    legacy = {   # 예전 설정 이름도 그대로 존중
        "scan": (cfg.get("discovery") or {}).get("scan_every_hours"),
        "kw": (cfg.get("keyword_discovery") or {}).get("every_hours"),
        "naver": cfg.get("search_refresh_hours"),
        "buzz": (cfg.get("social") or {}).get("buzz_refresh_hours"),
        "youtube": (cfg.get("social") or {}).get("youtube_refresh_hours"),
        "instagram": (cfg.get("social") or {}).get("instagram_refresh_hours"),
    }.get(src)
    every, off = SCHED_DEFAULT.get(src, (60, 0))
    if legacy and not (cfg.get("schedule") or {}):
        return int(float(legacy) * 60), off
    return every, off


def _kst_min(d):
    return int(d.timestamp() // 60) + 9 * 60


def slot_index(d, every, off):
    return (_kst_min(d) - off) // every


def slot_start(idx, every, off):
    m = idx * every + off - 9 * 60
    return dt.datetime.fromtimestamp(m * 60, KST)


def _pt(s):
    return dt.datetime.strptime(s[:16], "%Y-%m-%dT%H:%M").replace(tzinfo=KST)


def sched_of(hist, src):
    return hist.setdefault("sched", {}).setdefault(src, {})


def behind(hist, cfg, src):
    """지금 칸 기준으로 몇 칸 밀렸는지 (0이면 이번 칸은 이미 성공)."""
    every, off = cadence(cfg, src)
    s = sched_of(hist, src)
    last = s.get("ok")
    if not last:
        sid = STATUS_IDS.get(src, (src,))[0]
        last = hist.get("status", {}).get(sid, {}).get("lastSuccess")
    if not last:
        return 99
    return max(0, slot_index(now(), every, off) - slot_index(_pt(last), every, off))


def due(hist, cfg, src):
    if behind(hist, cfg, src) <= 0:
        return False
    every, _ = cadence(cfg, src)
    s = sched_of(hist, src)
    if s.get("fail_at") and now() - _pt(s["fail_at"]) < dt.timedelta(minutes=min(5, max(1, every // 2))):
        return False     # 방금 실패했으면 몇 분 쉬었다가 같은 칸 안에서 재시도
    return True


def mark(hist, src, ok):
    s = sched_of(hist, src)
    if ok:
        s["ok"] = iso(now())
        s["fail_at"], s["fails"] = None, 0
        keep = iso(now() - dt.timedelta(days=2))
        s["log"] = [t for t in s.get("log", []) if t >= keep] + [s["ok"]]
    else:
        s["fail_at"] = iso(now())
        s["fails"] = s.get("fails", 0) + 1


def compute_sla(hist, cfg):
    """최근 24시간 정시율 = 성공한 칸 수 ÷ 있어야 할 칸 수."""
    out, plan = {}, {}
    day_ago = iso(now() - dt.timedelta(hours=24))
    for src in SCHED_DEFAULT:
        every, off = cadence(cfg, src)
        plan[src] = {"every": every, "offset": off}
        s = hist.get("sched", {}).get(src)
        if not s:
            continue
        cur = slot_index(now(), every, off)
        expected = max(1, (24 * 60) // every)
        hit = len({slot_index(_pt(t), every, off) for t in s.get("log", []) if t >= day_ago}
                  & set(range(cur - expected + 1, cur + 1)))
        out[src] = {"every": every, "pct": round(100 * hit / expected), "hit": hit, "expected": expected,
                    "behind": behind(hist, cfg, src), "fails": s.get("fails", 0),
                    "next": iso(slot_start(cur + 1, every, off)), "last_ok": s.get("ok")}
    hist["sla"], hist["schedule"] = out, plan


# ---------------------------------------------------------------- 실시간 자료 작업
def task_seoul(cfg, keys, hist, new):
    places = [p for p in all_places(cfg, hist) if p.get("citydata_area")]

    def one(p):
        try:
            return fetch_seoul(keys["SEOUL_API_KEY"], p), None
        except Exception as e:
            return [], str(e)
    res = pmap(one, places, (cfg.get("schedule") or {}).get("_workers", 4))
    errs = [e for _, e in res if e]
    recs = [r for rs, _ in res for r in rs]
    new += recs
    set_status(hist, "seoul", bool(recs), "; ".join(errs), len(recs))
    log(f"서울 도시데이터 {len(recs)}건" + (f" / 오류: {'; '.join(errs)}" if errs else ""))
    if not recs:
        raise RuntimeError("; ".join(errs) or "자료 없음")
    return len(recs)


def rec_stations(cfg, hist):
    """추천 상위 장소의 측정소 (아직 추적하지 않는 곳도 현재값을 보기 위해)."""
    rc = cfg.get("recommend") or {}
    if not rc.get("fetch_air", True):
        return set()
    geo = hist.get("poi_geo", {})
    out = set()
    for r in (hist.get("recs") or {}).get("items", [])[:rc.get("air_top", 4)]:
        gu = r.get("gu") or (geo.get((r.get("poi") or {}).get("code") or "", {}) or {}).get("gu", "")
        if gu:
            out.add(gu)
            rs = roadside_for(cfg, hist, gu)
            if rs:
                out.add(rs)
    return out


def fetch_air_sido(key, sido="서울"):
    """시도별 실시간 측정정보: 서울 측정소 전체(도시대기·도로변)의 현재 1시간값을 한 번에."""
    k = key if "%" in key else urllib.parse.quote(key, safe="")
    url = ("https://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
           f"?serviceKey={k}&returnType=json&numOfRows=200&pageNo=1&sidoName={urllib.parse.quote(sido)}&ver=1.3")
    txt = http(url, timeout=30, retries=2)
    if txt.lstrip().startswith("<"):
        msg = txt.split("<returnAuthMsg>")[-1].split("<")[0] if "returnAuthMsg" in txt else txt[:200]
        raise RuntimeError(f"시도별 조회: {msg}")
    j = json.loads(txt)
    if find(j, "resultCode") not in (None, "00"):
        raise RuntimeError(f"시도별 조회: {find(j, 'resultMsg')}")
    out = {}
    for it in find(j, "items") or []:
        t = parse_time(it.get("dataTime"))
        if not t or not it.get("stationName"):
            continue
        out[it["stationName"]] = {"t": t, "mang": it.get("mangName", ""), "pm25": num(it.get("pm25Value")),
                                  "pm10": num(it.get("pm10Value")), "no2": num(it.get("no2Value")),
                                  "o3": num(it.get("o3Value")), "co": num(it.get("coValue"))}
    if not out:
        raise RuntimeError("시도별 조회: 자료 없음")
    return out


def task_air(cfg, keys, hist, new):
    """한 시간에 한 번: ① 서울 전체 측정소 현재값을 1회 호출로 받고 ② 빠진 시간이 있는 측정소만
    24시간치로 메운다. 예전(측정소마다 매번 호출)보다 호출 수가 약 10분의 1이라 하루 한도에 여유가 생긴다."""
    key = keys["AIRKOREA_API_KEY"]
    places = all_places(cfg, hist)
    now_tbl = hist.setdefault("station_now", {})
    bad = hist.setdefault("station_bad", {})
    week_ago = iso(now() - dt.timedelta(days=7))
    for s in [s for s, t in bad.items() if t < week_ago]:
        bad.pop(s)
    want = {"air_urban": {}, "air_road": {}}
    for p in places:
        for kind, field in (("air_urban", "air_urban"), ("air_road", "air_roadside")):
            s = (p.get(field) or "").strip()
            if s:
                want[kind].setdefault(s, []).append(p["id"])
    extra = rec_stations(cfg, hist)
    try:
        sido = fetch_air_sido(key)
    except Exception as e:
        sido = {}
        log(f"에어코리아 시도별 조회 실패 → 측정소별로 받음: {e}")
    if sido:
        hist["seoul_stations"] = {k: v["mang"] for k, v in sido.items()}
        for s, v in sido.items():
            if s in extra or any(s in w for w in want.values()):
                prev = now_tbl.get(s, {}).get("recent", [])
                rec = [v["t"], v["pm25"], v["no2"]]
                now_tbl[s] = {"kind": "air_road" if "도로" in v["mang"] else "air_urban", "mang": v["mang"],
                              "t": v["t"], "pm25": v["pm25"], "pm10": v["pm10"], "no2": v["no2"],
                              "recent": ([r for r in prev if r[0] != v["t"]] + [rec])[-6:]}
    last_t = {}
    for o in hist.get("obs", []):
        if o.get("src") in ("air_urban", "air_road") and o.get("station"):
            k2 = (o["src"], o["station"])
            if o["t"] > last_t.get(k2, ""):
                last_t[k2] = o["t"]
    total, backfill = 0, 0
    max_backfill = (cfg.get("schedule") or {}).get("_air_backfill_max", 8)
    for kind, stations in want.items():
        errs, n = [], 0
        for s, pids in stations.items():
            cur = sido.get(s)
            if sido and not cur:
                errs.append(f"{s}: 서울 측정소 목록에 없음 (이름 확인)")
                bad[s] = iso(now())
                continue
            gap = True
            if cur:
                prev = last_t.get((kind, s))
                gap = not prev or _pt(cur["t"]) - _pt(prev) > dt.timedelta(hours=1)
                for pid in pids:
                    new.append({"src": kind, "place": pid, "t": cur["t"], "station": s, "pm25": cur["pm25"],
                                "pm10": cur["pm10"], "no2": cur["no2"], "o3": cur["o3"], "co": cur["co"]})
                    n += 1
            if gap and backfill < max_backfill:      # 빠진 시간이 있을 때만 24시간치로 메움
                backfill += 1
                try:
                    recs = fetch_air(key, s, pids, kind)
                    new += recs
                    n += len(recs)
                except Exception as e:
                    if not cur:
                        errs.append(str(e))
        if stations:
            set_status(hist, kind, n > 0, "; ".join(errs), n)
            log(f"에어코리아 {kind} {n}건" + (f" / 오류: {'; '.join(errs)}" if errs else ""))
        total += n
    for s in extra:
        if sido and s not in sido:
            bad[s] = iso(now())
    try:
        pm_forecast_cycle(cfg, keys, hist, sido)
    except Exception as e:
        log(f"미세먼지 분포 예측 오류: {e}")
        traceback.print_exc()
    if not total and not sido:
        raise RuntimeError("에어코리아 자료 없음")
    return total


def task_recs_live(cfg, keys, hist):
    """추천 상위 장소 중 아직 추적하지 않는 곳의 인구·바람을 10분마다 가볍게 다시 읽는다."""
    rc = cfg.get("recommend") or {}
    tracked = {p.get("citydata_area") for p in all_places(cfg, hist)}
    codes = [r["poi"]["code"] for r in (hist.get("recs") or {}).get("items", [])[:rc.get("live_top", 5)]
             if r.get("poi") and r["poi"]["code"] not in tracked and r["poi"]["code"] not in hist["_scan_live"]]
    if not codes:
        return 0

    def one(code):
        try:
            return scan_place(keys["SEOUL_API_KEY"], code)
        except Exception:
            return None
    got = [r for r in pmap(one, codes, 4) if r]
    for r in got:
        hist["_scan_live"][r["code"]] = r
    return len(got)


# ---------------------------------------------------------------- 밈 추천 (사람이 몰리고, 미세먼지 변화를 잴 수 있는 밈)
def best_windows(rows):
    """[(t, pop)] → 평일/주말 각각 인구가 가장 많은 시각(‘중’ 측정)과 낮 시간대 가장 적은 시각(‘전’ 기준 측정)."""
    import statistics
    out = {}
    for we in (True, False):
        by_h = {}
        for t, pop in rows:
            if pop is None:
                continue
            h, w = _slot_hw(t)
            if w == we:
                by_h.setdefault(h, []).append(pop)
        med = {h: statistics.median(v) for h, v in by_h.items() if len(v) >= 2}
        day = {h: v for h, v in med.items() if 8 <= h <= 22}
        if len(day) < 4:
            continue
        peak = max(day, key=day.get)
        base = min(day, key=day.get)
        out["we" if we else "wd"] = {"peak_h": peak, "peak_pop": round(day[peak]), "base_h": base,
                                     "base_pop": round(day[base]), "swing": round(day[peak] / max(1, day[base]), 2),
                                     "hours": len(day)}
    return out


def balanced_rejects(rows, per_reason=12):
    """뺀 키워드를 이유별로 고르게 보여 준다 (한 이유가 목록을 다 차지하지 않게)."""
    by = {}
    for r in rows:
        by.setdefault(r["why"], []).append(r)
    out = []
    for why, rs in sorted(by.items(), key=lambda kv: -len(kv[1])):
        out += [dict(r, n_reason=len(rs)) for r in rs[:per_reason]]
    return out


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def recommend_memes(cfg, hist):
    import math
    rc = cfg.get("recommend") or {}
    kw = hist.get("keywords") or {}
    cands = kw.get("cands") or {}
    board = {b["code"]: b for b in (hist.get("discovery") or {}).get("board", [])}
    live = hist.get("_scan_live") or {}
    scan_rows = load_json("scan.json", {"rows": []}).get("rows", [])
    by_code = {}
    for r in scan_rows:
        by_code.setdefault(r["code"], []).append(r)
    trend_rel = {t["title"]: bool(t.get("places") or t.get("words")) for t in hist.get("trends", [])}
    rising_now = {r["term"] for r in kw.get("rising", [])}
    st_now = hist.get("station_now", {})
    places = all_places(cfg, hist)
    name_to_code = {b["name"]: c for c, b in board.items()}
    tracked = {}
    for p in places:
        code = p.get("citydata_area")
        code = name_to_code.get(code, code)
        if code:
            tracked[code] = p["id"]
        tracked[p["label"]] = p["id"]
    gpl = {e.get("place"): e for e in (hist.get("gplaces") or {}).values()}
    fresh = iso(now() - dt.timedelta(days=rc.get("max_age_days", 3)))
    geo = hist.get("poi_geo", {})
    # 측정소 현재값: 이번 실행의 값이 없으면 저장된 관측 중 가장 최근 값 (3시간 이내)
    recent_cut = iso(now() - dt.timedelta(hours=3))
    st_now = dict(st_now)
    last_seoul = {}
    for o in hist.get("obs", []):
        if o.get("src") in ("air_urban", "air_road") and o.get("station") and o["t"] >= recent_cut:
            cur = st_now.get(o["station"])
            if not cur or o["t"] > cur.get("t", ""):
                st_now[o["station"]] = {"t": o["t"], "pm25": o.get("pm25"), "no2": o.get("no2")}
        elif o.get("src") == "seoul" and o["t"] >= recent_cut:
            if o["t"] > last_seoul.get(o["place"], {}).get("t", ""):
                last_seoul[o["place"]] = o

    groups, rejected = {}, {}
    for term, e in cands.items():
        pl = e.get("place") or {}
        src = e.get("source", "")
        ok, why, kind = meme_gate(term, pl, src, trend_rel.get(term, src != "구글 급상승"))
        if not ok:
            rejected[term] = {"term": term, "why": why, "place": pl.get("name", ""), "source": src,
                              "cat": (pl.get("category") or "").split(">")[-1].strip()}
            continue
        if e.get("ev"):
            rejected[term] = {"term": term, "why": "주최 측 일정이 있는 행사 (밈이 아니라 대조군)", "place": pl.get("name", ""), "source": src}
            continue
        if e.get("last_seen", "") < fresh:
            continue
        cd = pl.get("citydata") or {}
        key = cd.get("code") or pl.get("name") or term
        g = groups.setdefault(key, {"place": pl, "terms": [], "kinds": set(), "sources": set(), "rise": 0.0,
                                    "hits": 0, "r3": 0.0, "rp": 0.0, "sat": False, "first": e.get("first", iso(now())),
                                    "seed": False})
        if pl.get("lat") and not g["place"].get("lat"):
            g["place"] = pl
        rise = 10.0 if e.get("saturated") else float(e.get("rise") or 0)
        g["terms"].append((term, rise))
        g["kinds"].add(kind)
        g["sources"].add(src)
        g["seed"] |= src == "직접 입력"
        if rise >= g["rise"]:
            g["rise"], g["r3"], g["rp"] = rise, e.get("r3", 0), e.get("rp", 0)
        g["hits"] = max(g["hits"], len(e.get("hits", [])))
        g["sat"] |= bool(e.get("saturated"))
        g["first"] = min(g["first"], e.get("first", g["first"]))

    items = []
    for key, g in groups.items():
        pl = g["place"]
        cd = pl.get("citydata") or {}
        code = cd.get("code")
        reasons, warns = [], []
        # ── ① 밈 신호 (35)
        m = min(1.0, math.log1p(g["rise"]) / math.log1p(10)) * 18 + min(1.0, g["hits"] / 4) * 7
        terms = [t for t, _ in sorted(g["terms"], key=lambda x: -x[1])]
        if any(t in rising_now for t in terms):
            m += 3
            reasons.append("지금 블로그·유튜브 제목에서 급상승 중")
        if "구글 급상승" in g["sources"]:
            m += 3
            reasons.append("구글 급상승 검색어와 겹침")
        if len(terms) >= 2:
            m += 4
            reasons.append(f"같은 장소를 가리키는 밈 키워드 {len(terms)}개")
        if g["kinds"] == {"지역명"}:
            m -= 6
            warns.append("지역 이름만 늘어남 — 구체적인 밈이 무엇인지 확인 필요")
        if g["seed"]:
            m += 2
        m = _clamp(m, 0, 35)
        if g["sat"]:
            stage = "정점 부근"
        elif g["rise"] >= 3 and g["rp"] < 5:
            stage = "초기 확산"
        elif g["rise"] >= 1.5:
            stage = "확산 중"
        elif g["rise"] < 1:
            stage = "식는 중"
        else:
            stage = "보합"
        if stage == "초기 확산":
            reasons.append("초기 확산 단계 — 정점 ‘전’ 기준선을 지금 잡을 수 있음")
        elif stage == "식는 중":
            warns.append("블로그 글이 줄어드는 중 — 정점이 지났을 수 있음")

        # ── ② 인구 측정 가능성 (35)
        c, crowd = 0.0, {}
        b = None
        if code and code in live:
            lv = live[code]
            b = {"pop": lv["pop"], "cg": lv.get("cg"), "t": lv["t"], "y": lv.get("y"), "wind": lv.get("_wind"),
                 "spd": lv.get("_spd"), "pm25": lv.get("_pm25")}
        elif code:
            b = board.get(code)
        if code:
            c += 12
            reasons.append(f"서울 실시간 인구 자료가 있는 장소 ({cd.get('name')})")
        else:
            warns.append("서울 주요 장소 121곳 밖 — 인구는 현장 계수로만 잴 수 있음")
        if b:
            ratio = b.get("ratio")
            if code in live:
                med, ymed, n = baseline_of(by_code.get(code, []), b["t"])
                ratio = round(b["pop"] / med, 2) if med else None
                dy = round((b.get("y") or 0) - ymed, 1) if ymed is not None else None
            else:
                dy = b.get("dy")
            crowd = {"pop": b.get("pop"), "ratio": ratio, "cg": b.get("cg"), "dy": dy, "t": b.get("t"),
                     "live": code in live}
            if ratio:
                c += _clamp((ratio - 1.0) / 0.6, 0, 1) * 12
                if ratio >= 1.3:
                    reasons.append(f"지금 평소보다 {ratio:.1f}배 몰림")
            if dy and dy > 0:
                c += _clamp(dy / 8, 0, 1) * 5
                if dy >= 3:
                    reasons.append(f"10·20대 비율 +{dy:.0f}%p")
            c += {3: 3, 4: 6}.get(b.get("cg") or 0, 0)
        c = _clamp(c, 0, 35)

        # ── ③ 미세먼지 변화 측정 가능성 (20)
        a, air = 0.0, {}
        gu = gu_of(pl, geo)
        urban = gu if gu else ""
        road = roadside_for(cfg, hist, gu) if gu else ""
        if urban:
            a += 5
        if road:
            a += 7
            reasons.append(f"도로변 측정소({road})와 도시대기({urban})를 짝지어 ΔNO₂를 볼 수 있음")
        else:
            road = ""
            warns.append("짝지을 도로변 측정소 없음 — 휴대 센서로 도로 쪽·안쪽을 같이 재세요")
        tid = tracked.get(code) or tracked.get(pl.get("name", ""))
        ls = last_seoul.get(tid) or {}
        wind = (b or {}).get("wind") if b and b.get("wind") is not None else ls.get("wind")
        spd = (b or {}).get("spd") if b and b.get("spd") is not None else ls.get("spd")
        if spd is not None:
            a += 2
        if wind is not None:
            if wind < 2:
                a += 4
                reasons.append(f"바람 약함({wind:g} m/s) — 지역 배출이 쌓여 차이가 잘 보임")
            elif wind < 3.5:
                a += 2
            else:
                warns.append(f"바람 강함({wind:g} m/s) — 희석되어 차이가 작게 나올 수 있음")
        u_now, r_now = st_now.get(urban) or {}, st_now.get(road) or {}
        bg = u_now.get("pm25") if u_now.get("pm25") is not None else (b or {}).get("pm25")
        if bg is not None:
            if bg > 35:
                a -= 3
                warns.append(f"도시 전체 PM2.5가 높음({bg:g}) — 장소 효과가 묻힐 수 있음")
            elif bg <= 15:
                a += 2
        dno2 = None
        if u_now.get("no2") is not None and r_now.get("no2") is not None and u_now.get("t") == r_now.get("t"):
            dno2 = round((r_now["no2"] - u_now["no2"]) * 1000, 1)
        air = {"urban": urban, "roadside": road, "pm25_u": u_now.get("pm25"), "no2_u": u_now.get("no2"),
               "pm25_r": r_now.get("pm25"), "no2_r": r_now.get("no2"), "t": u_now.get("t") or r_now.get("t"),
               "dno2": dno2, "wind": wind, "pm25_place": (b or {}).get("pm25") if b and b.get("pm25") is not None else ls.get("pm25"),
               "spd": spd}
        a = _clamp(a, 0, 20)

        # ── ④ 현장 실험 실용성 (10)
        rows = [(r["t"], r["pop"]) for r in by_code.get(code, [])] if code else []
        if tid:
            rows += [(o["t"], o.get("pop")) for o in hist.get("obs", []) if o.get("src") == "seoul" and o.get("place") == tid]
        win = best_windows(rows) if rows else {}
        p = 0
        if win:
            p += 4
        cat = pl.get("category", "")
        if code or any(x in cat for x in CAT_OUTDOOR):
            p += 3
        if tid:
            p += 3
            reasons.append("이미 추적 중 — 전·중·후 비교용 기록이 쌓이는 중")
        total = round(m + c + a + p)
        gp = gpl.get(tid) or gpl.get("rec:" + key) or {}
        items.append({
            "key": key, "label": pl.get("name") or cd.get("name") or key, "terms": terms[:6],
            "kinds": sorted(g["kinds"]), "sources": sorted(g["sources"]), "category": cat,
            "gu": gu, "addr": pl.get("addr", "") or (geo.get(code or "", {}) or {}).get("addr", ""),
            "lat": pl.get("lat") or (geo.get(code or "", {}) or {}).get("lat") or gp.get("lat"),
            "lon": pl.get("lon") or (geo.get(code or "", {}) or {}).get("lon") or gp.get("lon"),
            "kakao": pl.get("url", ""), "poi": {"code": code, "name": cd.get("name")} if code else None,
            "tracked_id": tid, "score": total,
            "parts": {"meme": round(m), "crowd": round(c), "air": round(a), "prac": p},
            "stage": stage, "reasons": reasons, "warnings": warns,
            "meme": {"rise": round(g["rise"], 2), "r3": g["r3"], "rp": g["rp"], "hits": g["hits"], "saturated": g["sat"],
                     "first": g["first"]},
            "crowd": crowd, "air": air, "windows": win,
            "gmaps": {"uri": gp.get("uri"), "rating": gp.get("rating"), "reviews": gp.get("reviews")} if gp else None,
        })
    items.sort(key=lambda x: -x["score"])
    keep = [x for x in items if x["score"] >= rc.get("min_score", 25)][:rc.get("top", 10)]
    hist["recs"] = {"at": iso(now()), "items": keep, "considered": len(cands), "grouped": len(items),
                    "rejected": balanced_rejects(rejected.values()),
                    "weights": {"meme": 35, "crowd": 35, "air": 20, "prac": 10}}
    return len(keep)


# ---------------------------------------------------------------- 미세먼지 분포: 밈 기반 예측 → 실제와 비교
# 방법 (연구 설명용):
#   ① 기준선(지속 예측): 지금(t) 서울 측정소 PM2.5가 한 시간 뒤(t+h)에도 그대로라고 본다.
#   ② 밈 효과: 추천된 밈 장소마다 '증가량 = α × 인구 초과(평소 대비−1) × 밈 신호(0~1) × 1/(1+풍속)'을
#      그 장소를 중심으로 가우스 모양(σ km)으로 퍼뜨려 기준선에 더한다.
#   ③ t+h의 실제 측정값이 들어오면 측정소 위치에서 오차·상관·정확도를, 격자에서 고농도 구역 일치도(IoU)를 계산한다.
#   ④ 기준선만 쓴 예측과 비교한 개선율(skill)로 '밈 정보가 실제로 도움이 되었는가'를 따로 잰다.
#   ⑤ α는 지난 예측들의 잔차(도시 전체 공통 변화를 뺀 값)로 최소제곱 보정한다.
import math as _math

PM_BBOX = (37.42, 37.71, 126.76, 127.19)   # 남, 북, 서, 동


def km_between(a_lat, a_lon, b_lat, b_lon):
    dy = (a_lat - b_lat) * 111.0
    dx = (a_lon - b_lon) * 111.0 * _math.cos(_math.radians((a_lat + b_lat) / 2))
    return _math.hypot(dx, dy)


def pm_cfg(cfg):
    d = {"enabled": True, "horizon_h": 1, "alpha": 6.0, "sigma_km": 1.0, "calibrate": True,
         "grid": [48, 40], "hotspot_q": 0.75, "keep": 96, "use_roadside": True, "mask_km": 6.0}
    d.update({k: v for k, v in (cfg.get("pm_map") or {}).items() if not k.startswith("_")})
    return d


def ensure_station_geo(keys, hist, names):
    """측정소 좌표: ① 에어코리아 측정소정보 API(별도 활용신청, 무료) ② 없으면 카카오로 구청·측정소 이름 위치를 대신 씀."""
    pm = hist.setdefault("pm_map", {})
    geo = pm.setdefault("stations", {})
    if not pm.get("geo_at") or pm["geo_at"] < iso(now() - dt.timedelta(days=30)):
        pm["geo_at"] = iso(now())
        try:
            k = keys["AIRKOREA_API_KEY"]
            k = k if "%" in k else urllib.parse.quote(k, safe="")
            txt = http("https://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList"
                       f"?serviceKey={k}&returnType=json&numOfRows=200&pageNo=1&addr={urllib.parse.quote('서울')}&ver=1.1",
                       timeout=30, retries=1)
            if not txt.lstrip().startswith("<"):
                for it in find(json.loads(txt), "items") or []:
                    a, b = num(it.get("dmX")), num(it.get("dmY"))
                    if a is None or b is None:
                        continue
                    lat, lon = (a, b) if 33 < a < 39 else (b, a)
                    geo[it.get("stationName")] = {"lat": lat, "lon": lon, "src": "에어코리아",
                                                  "mang": it.get("mangName", ""), "addr": it.get("addr", "")}
        except Exception as e:
            log(f"측정소 좌표(에어코리아 측정소정보) 실패 → 카카오로 대신: {e}")
    if keys.get("KAKAO_REST_API_KEY"):
        for name in [n for n in names if n not in geo][:40]:
            qs = [f"서울 {name}청"] if name.endswith("구") else [f"{name} 도로변대기측정소", f"서울 {name}"]
            for q in qs:
                try:
                    items = [it for it in kakao_local_search(keys, q, size=3) if (it.get("address_name") or "").startswith("서울")]
                except Exception:
                    items = []
                if items:
                    geo[name] = {"lat": float(items[0]["y"]), "lon": float(items[0]["x"]), "src": "카카오(근사)",
                                 "addr": items[0].get("address_name", "")}
                    break
    return geo


def idw(points, lat, lon, power=2.0):
    """points: [(lat, lon, value)] — 역거리가중 보간 (측정소 바로 위면 그 값)."""
    num_, den = 0.0, 0.0
    for a, b, v in points:
        d = km_between(lat, lon, a, b)
        if d < 0.05:
            return v
        w = 1.0 / d ** power
        num_ += w * v
        den += w
    return num_ / den if den else None


def meme_sources(cfg, hist):
    """추천 밈 장소 → 단위 증가량 u(α 곱하기 전)."""
    out = []
    for r in (hist.get("recs") or {}).get("items", []):
        if not (r.get("lat") and r.get("lon")):
            continue
        c = r.get("crowd") or {}
        ratio = c.get("ratio")
        crowd = _clamp((ratio - 1.0), 0, 2) if ratio else _clamp(((c.get("cg") or 2) - 2) / 2, 0, 1)
        meme = _clamp(((r.get("parts") or {}).get("meme") or 0) / 35, 0, 1)
        wind = (r.get("air") or {}).get("wind")
        wind = 2.0 if wind is None else wind
        u = crowd * meme / (1 + wind)
        out.append({"key": r["key"], "label": r["label"], "lat": r["lat"], "lon": r["lon"], "u": round(u, 4),
                    "ratio": ratio, "meme": round(meme, 2), "wind": wind, "score": r.get("score")})
    return out


def kernel_sum(sources, lat, lon, sigma):
    s = 0.0
    for src in sources:
        d = km_between(lat, lon, src["lat"], src["lon"])
        s += src["u"] * _math.exp(-(d * d) / (2 * sigma * sigma))
    return s


def pm_grid(stations_vals, geo, P, sources=None, alpha=0.0, sigma=1.0):
    """격자 값 목록 [[lat, lon, v]] (측정소에서 mask_km보다 먼 칸은 None)."""
    pts = [(geo[s]["lat"], geo[s]["lon"], v) for s, v in stations_vals.items() if s in geo and v is not None]
    if len(pts) < 3:
        return []
    s_lat, n_lat, w_lon, e_lon = PM_BBOX
    ny, nx = P["grid"][1], P["grid"][0]
    out = []
    for j in range(ny):
        lat = s_lat + (n_lat - s_lat) * (j + 0.5) / ny
        for i in range(nx):
            lon = w_lon + (e_lon - w_lon) * (i + 0.5) / nx
            near = min(km_between(lat, lon, a, b) for a, b, _ in pts)
            if near > P["mask_km"]:
                out.append(None)
                continue
            v = idw(pts, lat, lon)
            if sources:
                v += alpha * kernel_sum(sources, lat, lon, sigma)
            out.append(v)
    return out


def _rank(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for k in range(i, j + 1):
            r[order[k]] = (i + j) / 2 + 1
        i = j + 1
    return r


def _pearson(a, b):
    n = len(a)
    if n < 3:
        return None
    ma, mb = sum(a) / n, sum(b) / n
    sa = _math.sqrt(sum((x - ma) ** 2 for x in a))
    sb = _math.sqrt(sum((y - mb) ** 2 for y in b))
    if sa == 0 or sb == 0:
        return None
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (sa * sb)


def _quantile(xs, q):
    s = sorted(xs)
    if not s:
        return None
    k = (len(s) - 1) * q
    f = int(k)
    return s[f] + (s[min(f + 1, len(s) - 1)] - s[f]) * (k - f)


def evaluate_forecast(fc, actual, geo, P):
    """fc: 예측 기록, actual: {측정소: t+h 실제 PM2.5}."""
    import statistics
    names = [s for s in fc["pred_st"] if actual.get(s) is not None and fc["base"].get(s) is not None]
    if len(names) < 5:
        return None
    pr = [fc["pred_st"][s] for s in names]
    ac = [actual[s] for s in names]
    bs = [fc["base"][s] for s in names]
    err = [p - a for p, a in zip(pr, ac)]
    mae = sum(abs(e) for e in err) / len(err)
    rmse = _math.sqrt(sum(e * e for e in err) / len(err))
    rmse_p = _math.sqrt(sum((b - a) ** 2 for b, a in zip(bs, ac)) / len(ac))
    mean_a = sum(ac) / len(ac)
    r = _pearson(pr, ac)
    rho = _pearson(_rank(pr), _rank(ac))
    # 격자 고농도 구역 일치도 (예상 상위 25% 칸 vs 실제 상위 25% 칸)
    g_pred = pm_grid(fc["base"], geo, P, fc["sources"], fc["alpha"], fc["sigma"])
    g_act = pm_grid(actual, geo, P)
    iou = None
    if g_pred and g_act:
        cells = [(p, a) for p, a in zip(g_pred, g_act) if p is not None and a is not None]
        if cells:
            tp, ta = _quantile([p for p, _ in cells], P["hotspot_q"]), _quantile([a for _, a in cells], P["hotspot_q"])
            A = {i for i, (p, _) in enumerate(cells) if p >= tp}
            B = {i for i, (_, a) in enumerate(cells) if a >= ta}
            iou = len(A & B) / len(A | B) if A | B else None
    # 밈 장소가 실제로 도시 중앙값보다 짙었나 (적중률)
    pts = [(geo[s]["lat"], geo[s]["lon"], actual[s]) for s in actual if s in geo and actual[s] is not None]
    med = statistics.median([v for *_, v in pts]) if pts else None
    hits = [idw(pts, s["lat"], s["lon"]) > med for s in fc["sources"] if s["u"] > 0] if med is not None else []
    # 보정용: 공통 변화(중앙값)를 뺀 잔차 × 밈 특성값
    resid = [a - b for a, b in zip(ac, bs)]
    common = statistics.median(resid)
    feats = [kernel_sum(fc["sources"], geo[s]["lat"], geo[s]["lon"], fc["sigma"]) for s in names]
    return {"n": len(names), "mae": round(mae, 2), "rmse": round(rmse, 2), "bias": round(sum(err) / len(err), 2),
            "r": None if r is None else round(r, 3), "rho": None if rho is None else round(rho, 3),
            "acc": round(max(0.0, 100 * (1 - mae / mean_a)), 1) if mean_a > 0 else None,
            "iou": None if iou is None else round(100 * iou, 1),
            "rmse_persist": round(rmse_p, 2),
            "skill": round(100 * (1 - rmse / rmse_p), 1) if rmse_p > 0 else 0.0,
            "hit": round(100 * sum(hits) / len(hits), 1) if hits else None, "n_hit": len(hits),
            "sum_rf": round(sum((x - common) * f for x, f in zip(resid, feats)), 4),
            "sum_ff": round(sum(f * f for f in feats), 6), "actual_t": fc["target"]}


def pm_forecast_cycle(cfg, keys, hist, sido):
    """에어코리아 시도별 값이 들어올 때마다: 실제값 저장 → 끝난 예측 채점 → α 보정 → 다음 예측 발행."""
    P = pm_cfg(cfg)
    if not P["enabled"] or not sido:
        return
    pm = hist.setdefault("pm_map", {})
    keep_names = [s for s, v in sido.items() if P["use_roadside"] or "도로" not in v.get("mang", "")]
    geo = ensure_station_geo(keys, hist, keep_names)
    t = max(v["t"] for v in sido.values())
    vals = {s: sido[s]["pm25"] for s in keep_names if sido[s]["t"] == t and sido[s]["pm25"] is not None}
    act = pm.setdefault("actual", {})
    act[t] = {s: v for s, v in vals.items()}
    cut = iso(now() - dt.timedelta(hours=P["keep"]))
    pm["actual"] = {k: v for k, v in act.items() if k >= cut}
    # 채점
    fcs = pm.setdefault("forecasts", [])
    for fc in fcs:
        if fc.get("eval") is None and fc["target"] in pm["actual"]:
            fc["eval"] = evaluate_forecast(fc, pm["actual"][fc["target"]], geo, P)
    # α 보정 (최소제곱, 0~30 µg/m³)
    alpha = P["alpha"]
    ev = [f["eval"] for f in fcs if f.get("eval")]
    srf, sff = sum(e["sum_rf"] for e in ev), sum(e["sum_ff"] for e in ev)
    if P["calibrate"] and sff > 0.05 and len(ev) >= 6:
        alpha = round(_clamp(srf / sff, 0.0, 30.0), 2)
    pm["calib"] = {"alpha": alpha, "n": len(ev), "sum_ff": round(sff, 4), "default": P["alpha"],
                   "calibrated": P["calibrate"] and sff > 0.05 and len(ev) >= 6}
    # 다음 예측 발행 (같은 기준 시각이면 한 번만)
    target = iso(_pt(t) + dt.timedelta(hours=P["horizon_h"]))
    if not any(f["issued"] == t for f in fcs):
        srcs = meme_sources(cfg, hist)
        sigma = P["sigma_km"]
        pred = {}
        for s, v in vals.items():
            if s in geo:
                pred[s] = round(v + alpha * kernel_sum(srcs, geo[s]["lat"], geo[s]["lon"], sigma), 2)
        fcs.append({"issued": t, "target": target, "made": iso(now()), "alpha": alpha, "sigma": sigma,
                    "base": vals, "pred_st": pred, "sources": srcs, "eval": None})
    pm["forecasts"] = [f for f in fcs if f["issued"] >= cut][-P["keep"]:]
    pm["grid"] = {"bbox": PM_BBOX, "nx": P["grid"][0], "ny": P["grid"][1], "mask_km": P["mask_km"],
                  "hotspot_q": P["hotspot_q"]}
    done = [f["eval"] for f in pm["forecasts"] if f.get("eval")]
    if done:
        def avg(k):
            xs = [e[k] for e in done if e.get(k) is not None]
            return round(sum(xs) / len(xs), 2) if xs else None
        pm["summary"] = {k: avg(k) for k in ("acc", "iou", "r", "rho", "mae", "rmse", "skill", "hit")}
        pm["summary"]["n"] = len(done)
    log(f"미세먼지 분포 예측: 기준 {t} → {target}, 채점 완료 {len(done)}건, α={alpha}")


# ---------------------------------------------------------------- 실행
def run_once(cfg, keys, budget_sec=None):
    """실시간 자료(도시데이터·에어코리아)를 먼저, 느린 자료는 많이 밀린 순서로.
    한 번 실행 시간이 예산(기본 240초)을 넘으면 남은 느린 작업은 다음 실행으로 넘겨, 다음 10분 칸의
    실시간 수집이 밀리지 않게 한다."""
    t0 = time.monotonic()
    budget = budget_sec if budget_sec is not None else (cfg.get("schedule") or {}).get("_run_budget_sec", 240)
    hist = load_history()
    quota_init(hist, cfg)
    hist["_scan_live"] = {}
    d = cfg.get("discovery", {})
    kd = cfg.get("keyword_discovery", {})
    soc_need = {"youtube", "instagram", "gplaces", "x"}       # 선택 항목: 키 없으면 상태 표시 안 함
    new, ran, deferred = [], [], []

    def attempt(src, fn, status=True):
        """status=False: 작업이 스스로 상태를 남긴다(부분 실패 메시지 보존). 그러지 못하고 죽었을 때만 대신 남김."""
        sids = STATUS_IDS.get(src, (src,))
        before = {sid: dict(hist["status"].get(sid, {})) for sid in sids}
        try:
            n = fn()
            if status:
                set_status(hist, src, True, "", n or 0)
            mark(hist, src, True)
            ran.append(src)
            log(f"{src} 완료 ({n})")
        except Exception as e:
            for sid in sids:
                if src == "recs_live":
                    break
                if status or hist["status"].get(sid, {}) == before[sid]:
                    set_status(hist, sid, False, str(e))
            mark(hist, src, False)
            log(f"{src} 오류: {e}")

    # ── 1) 실시간: 예산과 무관하게 항상 먼저
    if keys.get("SEOUL_API_KEY"):
        if due(hist, cfg, "seoul"):
            attempt("seoul", lambda: task_seoul(cfg, keys, hist, new), status=False)
    else:
        set_status(hist, "seoul", False, "인증키 없음 (SEOUL_API_KEY)")
    if keys.get("AIRKOREA_API_KEY"):
        if due(hist, cfg, "air"):
            attempt("air", lambda: task_air(cfg, keys, hist, new), status=False)
    else:
        set_status(hist, "air_urban", False, "인증키 없음 (AIRKOREA_API_KEY)")
        set_status(hist, "air_road", False, "인증키 없음 (AIRKOREA_API_KEY)")

    # ── 2) 느린 자료: (이름, 켜짐 여부, 필요한 키 안내, 함수)
    naver_ok = keys.get("NAVER_CLIENT_ID") and keys.get("NAVER_CLIENT_SECRET")

    def naver_job():
        hist["search"] = fetch_naver(keys["NAVER_CLIENT_ID"], keys["NAVER_CLIENT_SECRET"],
                                     all_groups(cfg, hist), cfg.get("search_days", 90))
        return len(hist["search"])

    def x_job():
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
        if errs:
            raise RuntimeError("; ".join(errs))
        return len(cfg["x_queries"])

    def scan_job():
        n = run_scan(cfg, keys["SEOUL_API_KEY"], hist)
        auto_track(cfg, hist)
        return n

    def trends_job():
        fetch_trends(hist, cfg)
        return len(hist.get("trends", []))

    slow = [
        ("trends", d.get("enabled", True), True, "", trends_job),
        ("scan", d.get("enabled", True), keys.get("SEOUL_API_KEY"), "인증키 없음 (SEOUL_API_KEY)", scan_job),
        ("kw", kd.get("enabled", True), naver_ok, "인증키 없음 (NAVER_CLIENT_ID / NAVER_CLIENT_SECRET, 검색 API 추가 필요)",
         lambda: keyword_discovery(cfg, keys, hist)),
        ("naver", True, naver_ok, "인증키 없음 (NAVER_CLIENT_ID / NAVER_CLIENT_SECRET)", naver_job),
        ("buzz", True, naver_ok, "인증키 없음 (NAVER_CLIENT_ID / NAVER_CLIENT_SECRET (검색 API 추가 필요))",
         lambda: fetch_naver_buzz(cfg, keys, hist)),
        ("youtube", True, keys.get("YOUTUBE_API_KEY"), "", lambda: fetch_youtube(cfg, keys, hist)),
        ("instagram", True, keys.get("IG_USER_ID") and keys.get("IG_ACCESS_TOKEN") and cfg.get("instagram_hashtags"), "",
         lambda: fetch_instagram(cfg, keys, hist)),
        ("gplaces", True, keys.get("GOOGLE_MAPS_API_KEY"), "", lambda: fetch_google_places(cfg, keys, hist)),
        ("kakao", bool(cfg.get("kakao_store", False)), keys.get("KAKAO_REST_API_KEY"), "인증키 없음 (KAKAO_REST_API_KEY)", lambda: fetch_kakao(cfg, keys, hist)),
        ("x", True, keys.get("X_BEARER_TOKEN") and cfg.get("x_queries"), "", x_job),
    ]
    order = {s[0]: i for i, s in enumerate(slow)}
    todo = []
    for src, enabled, ok, need, fn in slow:
        if not enabled:
            continue
        if not ok:
            if src in soc_need:
                hist["status"].pop(src, None)
            else:
                set_status(hist, src, False, need)
            continue
        extra = src == "naver" and ({g["name"] for g in all_groups(cfg, hist)} - set(hist.get("search", {})))
        if due(hist, cfg, src) or extra:
            todo.append((src, fn))
    todo.sort(key=lambda x: (-min(behind(hist, cfg, x[0]), 6), order[x[0]]))   # 많이 밀린 것부터
    for src, fn in todo:
        if time.monotonic() - t0 > budget:
            deferred.append(src)
            continue
        attempt(src, fn)
    if deferred:
        log(f"실행 시간 예산 {budget}초를 넘어 다음 실행으로 미룸: {', '.join(deferred)}")

    # ── 3) 추천: 상위 장소 실시간 인구 갱신 → 순위 다시 계산 (API 호출 없음, 매번)
    if keys.get("SEOUL_API_KEY") and due(hist, cfg, "recs_live"):
        attempt("recs_live", lambda: task_recs_live(cfg, keys, hist), status=False)
    try:
        n = recommend_memes(cfg, hist)
        set_status(hist, "recs", True, "", n)
    except Exception as e:
        set_status(hist, "recs", False, str(e))
        log(f"추천 계산 오류: {e}")
        traceback.print_exc()

    hist["obs"] = compact(hist["obs"] + new, cfg.get("keep_days", 60), cfg.get("full_resolution_days", 7))
    places = all_places(cfg, hist)
    for p in places:
        p.setdefault("kind", "pinned")
    hist["places"] = places
    hist["keyword_groups"] = all_groups(cfg, hist)
    hist["generatedAt"] = iso(now())
    hist["runs"] = (hist.get("runs", []) + [{"at": hist["generatedAt"], "new": len(new),
                                             "sec": round(time.monotonic() - t0), "ran": ran, "deferred": deferred}])[-200:]
    compute_sla(hist, cfg)
    gm = cfg.get("google_maps") or {}
    hist["maps"] = {"embed_key": gm.get("embed_key", ""), "on": bool(keys.get("GOOGLE_MAPS_API_KEY"))}
    hist["schema"] = SCHEMA
    hist.pop("_scan_live", None)
    save(hist)
    # 바뀐 것이 있을 때만 사이트를 다시 게시하도록 표시 (GitHub Pages 빌드·전송량 절약)
    with open(os.path.join(DATA_DIR, ".changed"), "w") as f:
        f.write("1" if (new or ran) else "0")
    log(f"저장 완료 ({round(time.monotonic() - t0)}초): 누적 관측 {len(hist['obs'])}건, 실행 {ran}, 미룸 {deferred}")


def check(cfg, keys):
    print("설정 점검")
    for k in ("SEOUL_API_KEY", "AIRKOREA_API_KEY", "NAVER_CLIENT_ID", "NAVER_CLIENT_SECRET", "X_BEARER_TOKEN",
              "YOUTUBE_API_KEY", "IG_USER_ID", "IG_ACCESS_TOKEN", "GOOGLE_MAPS_API_KEY","KAKAO_REST_API_KEY"):
        v = keys.get(k)
        print(f"  {k:22s} {'있음 (' + v[:4] + '…)' if v else '없음'}")
    print(f"  장소 {len(cfg['places'])}곳, 검색어 그룹 {len(cfg['keyword_groups'])}개")
    for p in cfg["places"]:
        print(f"   - {p['label']}: 도시데이터 '{p.get('citydata_area')}', "
              f"도시대기 '{p.get('air_urban')}', 도로변 '{p.get('air_roadside') or '없음'}'")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", type=int, default=0, help="초 단위 반복 간격 (예: 600). 시계 정각 칸에 맞춰 돈다")
    ap.add_argument("--until", type=int, default=0, help="--loop와 함께: 이 분이 지나면 끝냄 (GitHub 실행용)")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--plan", action="store_true", help="자료원별 갱신 시간표 출력")
    ap.add_argument("--recs-only", action="store_true", help="API 호출 없이 추천만 다시 계산 (시험용)")
    ap.add_argument("--config", default=os.path.join(HERE, "config.json"))
    a = ap.parse_args()
    with open(a.config, encoding="utf-8") as f:
        cfg = json.load(f)
    keys = load_keys()
    if a.check:
        check(cfg, keys)
        return
    if a.plan:
        for src in SCHED_DEFAULT:
            every, off = cadence(cfg, src)
            nxt = slot_start(slot_index(now(), every, off) + 1, every, off)
            print(f"  {src:10s} {every:5d}분마다 (오프셋 {off}분)  다음 칸 {nxt.strftime('%m-%d %H:%M')}")
        return
    if a.recs_only:
        hist = load_history()
        hist["_scan_live"] = {}
        n = recommend_memes(cfg, hist)
        hist.pop("_scan_live", None)
        compute_sla(hist, cfg)
        save(hist)
        log(f"추천 {n}개 다시 계산")
        return
    start = time.monotonic()
    while True:
        try:
            run_once(cfg, keys)
        except Exception:
            traceback.print_exc()
        if not a.loop:
            break
        # 실행 시간만큼 밀리지 않도록 다음 정각 칸(예: 10:10, 10:20 …)의 1분 뒤에 맞춰 깬다
        t = time.time()
        wake = (int(t) // a.loop + 1) * a.loop + 60
        if a.until and (time.monotonic() - start) + (wake - t) > a.until * 60:
            log("정해진 실행 시간이 끝나 종료합니다")
            break
        log(f"{dt.datetime.fromtimestamp(wake, KST).strftime('%H:%M')}에 다시 수집합니다 (종료: Ctrl+C)")
        time.sleep(max(1, wake - t))


if __name__ == "__main__":
    sys.exit(main())
