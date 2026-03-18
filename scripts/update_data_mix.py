#!/usr/bin/env python3
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LATEST_PATH = DATA_DIR / "latest.json"
STATUS_PATH = DATA_DIR / "status.json"

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
PMI_MANUAL_VALUE = os.getenv("PMI_MANUAL_VALUE", "").strip()
PMI_MANUAL_DATE = os.getenv("PMI_MANUAL_DATE", "").strip()

FRED_API_BASE = "https://api.stlouisfed.org/fred/series/observations"
CNN_FG_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
STOOQ_GOLD_URL = "https://stooq.com/q/d/l/?s=xauusd&i=d"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
errors = []

def log(*args):
    print(*args, flush=True)

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def read_previous():
    if LATEST_PATH.exists():
        try:
            return json.loads(LATEST_PATH.read_text())
        except Exception:
            return {}
    return {}

PREV = read_previous()
PREV_TRUSTED = PREV.get("meta", {}).get("source") == "scripts/update_data_mix.py"

def prev_get(*keys):
    if not PREV_TRUSTED:
        return None
    cur = PREV
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def curl_request(url, timeout=20, extra_headers=None):
    cmd = [
        "curl",
        "-L",
        "--compressed",
        "-sS",
        "--connect-timeout", "8",
        "--max-time", str(timeout),
        "--retry", "0",
        "-A", UA,
        "-w", "\n__HTTP_STATUS__:%{http_code}",
    ]
    for h in (extra_headers or []):
        cmd += ["-H", h]
    cmd.append(url)

    res = subprocess.run(cmd, capture_output=True, text=True)

    if res.returncode != 0 and not res.stdout:
        raise RuntimeError(res.stderr.strip() or f"curl 실패: {url}")

    raw = res.stdout or ""
    body, sep, status_text = raw.rpartition("\n__HTTP_STATUS__:")
    if not sep:
        body = raw
        status = None
    else:
        try:
            status = int(status_text.strip())
        except Exception:
            status = None

    return body, status

def curl_json(url, timeout=20, extra_headers=None):
    body, status = curl_request(url, timeout=timeout, extra_headers=extra_headers)
    try:
        data = json.loads(body)
    except Exception:
        snippet = body[:400].replace("\n", " ")
        if status and status >= 400:
            raise RuntimeError(f"HTTP {status} 응답 파싱 실패: {snippet}")
        raise RuntimeError(f"JSON 파싱 실패: {snippet}")

    if status and status >= 400:
        msg = data.get("error_message") or data.get("message") or str(data)
        raise RuntimeError(f"HTTP {status}: {msg}")

    return data

def curl_text(url, timeout=20, extra_headers=None):
    body, status = curl_request(url, timeout=timeout, extra_headers=extra_headers)
    if status and status >= 400:
        snippet = body[:300].replace("\n", " ")
        raise RuntimeError(f"HTTP {status}: {snippet}")
    return body

def fred_observations(series_id, limit=24):
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY 없음")

    qs = urlencode({
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": str(limit),
    })
    url = f"{FRED_API_BASE}?{qs}"
    data = curl_json(url, timeout=20)

    out = []
    for row in data.get("observations", []):
        d = row.get("date")
        v = safe_float(row.get("value"))
        if d and v is not None:
            out.append({"date": d, "value": v})

    if not out:
        raise RuntimeError(f"FRED API 빈 응답: {series_id}")

    out.reverse()
    return out

def load_first_available(series_ids, limit=24):
    last_error = None
    for sid in series_ids:
        try:
            return sid, fred_observations(sid, limit=limit)
        except Exception as e:
            last_error = e
            errors.append(f"{sid}: 실패 ({e})")
    raise last_error or RuntimeError("series load failed")

def transform_value(data, transform=None):
    if not data:
        return {"value": None, "date": None}

    last = data[-1]
    value = last["value"]

    if transform == "mom":
        if len(data) < 2:
            return {"value": None, "date": last["date"]}
        prev = data[-2]["value"]
        value = round(((last["value"] - prev) / prev) * 100, 2) if prev else None
    elif transform == "div1000":
        value = round(last["value"] / 1000, 2)
    else:
        value = round(float(last["value"]), 2)

    return {"value": value, "date": last["date"]}

def latest_common_ratio(series_a, series_b):
    map_a = {x["date"]: x["value"] for x in series_a}
    map_b = {x["date"]: x["value"] for x in series_b}
    common = sorted(set(map_a.keys()) & set(map_b.keys()))
    if not common:
        raise RuntimeError("공통 날짜 없음")
    d = common[-1]
    return d, map_a[d], map_b[d]

def fetch_buffett_proxy():
    equities = fred_observations("NCBEILQ027S", limit=12)
    gdp = fred_observations("GDP", limit=12)
    d, eq_val, gdp_val = latest_common_ratio(equities, gdp)
    ratio = round((eq_val / (gdp_val * 1000.0)) * 100.0, 2)
    return {"value": ratio, "date": d}

def fetch_gold_stooq():
    text = curl_text(STOOQ_GOLD_URL, timeout=20)
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    if len(lines) < 2:
        raise RuntimeError("Stooq gold CSV 빈 응답")

    data_rows = []
    for line in lines[1:]:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 5:
            continue
        dt = parts[0]
        close = safe_float(parts[4])
        if dt and close is not None:
            data_rows.append((dt, close))

    if not data_rows:
        raise RuntimeError("Stooq gold 파싱 실패")

    dt, price = data_rows[-1]
    return {"value": round(price, 2), "date": dt}

def shift_month(year, month, delta):
    total = year * 12 + (month - 1) + delta
    new_year = total // 12
    new_month = (total % 12) + 1
    return new_year, new_month

def fetch_pmi_manual():
    if not PMI_MANUAL_VALUE:
        raise RuntimeError("PMI_MANUAL_VALUE 없음")
    value = float(PMI_MANUAL_VALUE)
    date = PMI_MANUAL_DATE or now_iso()[:10]
    if len(date) == 7:
        date = date + "-01"
    return {"value": round(value, 1), "date": date}

def fetch_ism_pmi():
    now = datetime.now(timezone.utc)
    label_months = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"
    ]
    candidates = []

    for delta in (0, -1, -2, -3, -4):
        y, m = shift_month(now.year, now.month, delta)
        month_slug = label_months[m - 1]
        candidates.append((
            f"{y:04d}-{m:02d}-01",
            f"https://www.ismworld.org/supply-management-news-and-reports/news-publications/inside-supply-management-magazine/blog/{y}/{y:04d}-{m:02d}/ism-pmi-reports-roundup-{month_slug}-{y}-manufacturing/"
        ))
        candidates.append((
            f"{y:04d}-{m:02d}-01",
            f"https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/{month_slug}/"
        ))

    patterns = [
        r"Manufacturing PMI(?:®)?(?:\s+at|\s+registered at)?\s*([0-9]+(?:\.[0-9]+)?)\s*%",
        r"Manufacturing PMI(?:®)?(?:\s+at|\s+registered at)?\s*([0-9]+(?:\.[0-9]+)?)\s*percent",
        r"PMI(?:®)?.{0,80}?([0-9]+(?:\.[0-9]+)?)\s*%",
        r"PMI(?:®)?.{0,80}?([0-9]+(?:\.[0-9]+)?)\s*percent",
        r"registering\s+([0-9]+(?:\.[0-9]+)?)\s*%",
        r"registering\s+([0-9]+(?:\.[0-9]+)?)\s*percent",
    ]

    for report_date, url in candidates:
        try:
            html = curl_text(url, timeout=20)
        except Exception:
            continue

        html = re.sub(r"<script.*?</script>", " ", html, flags=re.I | re.S)
        html = re.sub(r"<style.*?</style>", " ", html, flags=re.I | re.S)
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"&nbsp;|&#160;", " ", text)
        text = re.sub(r"\s+", " ", text)

        for pat in patterns:
            mobj = re.search(pat, text, flags=re.I | re.S)
            if mobj:
                val = float(mobj.group(1))
                if 30 <= val <= 80:
                    return {"value": round(val, 1), "date": report_date}

    raise RuntimeError("ISM PMI 파싱 실패")

def fetch_conference_board_lei():
    url = "https://www.conference-board.org/topics/us-leading-indicators"
    html = curl_text(url, timeout=20)
    text = re.sub(r"<script.*?</script>", " ", html, flags=re.I | re.S)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)

    mobj = re.search(
        r"Leading Economic Index.*?(declined|increased|fell|rose)\s+by\s+([0-9]+(?:\.[0-9]+)?)%\s+in\s+([A-Za-z]+)\s+([0-9]{4})\s+to\s+([0-9]+(?:\.[0-9]+)?)",
        text,
        flags=re.I | re.S
    )
    if not mobj:
        raise RuntimeError("Conference Board LEI 파싱 실패")

    direction = mobj.group(1).lower()
    mom = float(mobj.group(2))
    if direction in ("declined", "fell"):
        mom = -mom

    month_name = mobj.group(3).lower()
    year = int(mobj.group(4))
    month_map = {
        "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
        "july":7,"august":8,"september":9,"october":10,"november":11,"december":12
    }
    month = month_map.get(month_name)
    if not month:
        raise RuntimeError("Conference Board LEI 월 파싱 실패")

    return {"value": round(mom, 2), "date": f"{year:04d}-{month:02d}-01"}

def fetch_cnn_fear_greed():
    data = curl_json(
        CNN_FG_URL,
        timeout=20,
        extra_headers=[
            "accept: application/json, text/plain, */*",
            "referer: https://www.cnn.com/markets/fear-and-greed",
            "origin: https://www.cnn.com",
        ],
    )
    fg = data.get("fear_and_greed") or data

    return {
        "score": fg.get("score"),
        "rating": fg.get("rating"),
        "timestamp": fg.get("timestamp"),
        "date": fg.get("timestamp"),
        "previousWeek": fg.get("previous_week"),
        "previousMonth": fg.get("previous_month"),
        "marketMomentum": fg.get("market_momentum_sp500"),
        "stockPriceStrength": fg.get("stock_price_strength"),
        "stockPriceBreadth": fg.get("stock_price_breadth"),
        "putCallOptions": fg.get("put_call_options"),
        "marketVolatility": fg.get("market_volatility_vix"),
        "junkBondDemand": fg.get("junk_bond_demand"),
        "safeHavenDemand": fg.get("safe_haven_demand"),
    }

def fetch_or_prev(label, fetch_fn, prev_path, default_obj):
    try:
        log("가져오는 중:", label)
        return fetch_fn()
    except Exception as e:
        errors.append(f"{label}: {e}")
        old = prev_get(*prev_path)
        if old is not None:
            log("이전값 사용:", label)
            return old
        log("빈값 처리:", label)
        return default_obj

def build_payload():
    market = {
        "sp": fetch_or_prev(
            "market.sp",
            lambda: transform_value(fred_observations("SP500", limit=10)),
            ("market", "sp"),
            {"value": None, "date": None},
        ),
        "nd": fetch_or_prev(
            "market.nd",
            lambda: transform_value(load_first_available(["NASDAQ100", "NASDAQCOM"], limit=10)[1]),
            ("market", "nd"),
            {"value": None, "date": None},
        ),
        "ks": fetch_or_prev(
            "market.ks",
            lambda: transform_value(fred_observations("SPASTT01KRM657N", limit=6)),
            ("market", "ks"),
            {"value": None, "date": None},
        ),
        "go": fetch_or_prev(
            "market.go",
            fetch_gold_stooq,
            ("market", "go"),
            {"value": None, "date": None},
        ),
        "dx": fetch_or_prev(
            "market.dx",
            lambda: transform_value(fred_observations("DTWEXBGS", limit=10)),
            ("market", "dx"),
            {"value": None, "date": None},
        ),
        "vx": fetch_or_prev(
            "market.vx",
            lambda: transform_value(fred_observations("VIXCLS", limit=10)),
            ("market", "vx"),
            {"value": None, "date": None},
        ),
        "bdi": fetch_or_prev(
            "market.bdi",
            lambda: transform_value(fred_observations("FRGSHPUSM649NCIS", limit=6)),
            ("market", "bdi"),
            {"value": None, "date": None},
        ),
    }

    core = {
        "lei": fetch_or_prev(
            "core.lei",
            fetch_conference_board_lei,
            ("core", "lei"),
            {"value": None, "date": None},
        ),
        "pmi": fetch_or_prev(
            "core.pmi",
            lambda: fetch_pmi_manual() if PMI_MANUAL_VALUE else fetch_ism_pmi(),
            ("core", "pmi"),
            {"value": None, "date": None},
        ),
        "buffett": fetch_or_prev(
            "core.buffett",
            fetch_buffett_proxy,
            ("core", "buffett"),
            {"value": None, "date": None},
        ),
        "jolts": fetch_or_prev(
            "core.jolts",
            lambda: transform_value(fred_observations("JTSJOL", limit=6), "div1000"),
            ("core", "jolts"),
            {"value": None, "date": None},
        ),
        "marginDebt": fetch_or_prev(
            "core.marginDebt",
            lambda: transform_value(fred_observations("BOGZ1FL663067003Q", limit=8), "div1000"),
            ("core", "marginDebt"),
            {"value": None, "date": None},
        ),
        "michigan": fetch_or_prev(
            "core.michigan",
            lambda: transform_value(fred_observations("UMCSENT", limit=6)),
            ("core", "michigan"),
            {"value": None, "date": None},
        ),
    }

    fear_greed = fetch_or_prev(
        "fearGreed",
        fetch_cnn_fear_greed,
        ("fearGreed",),
        {
            "score": None,
            "rating": None,
            "timestamp": None,
            "date": None,
            "previousWeek": None,
            "previousMonth": None,
            "marketMomentum": None,
            "stockPriceStrength": None,
            "stockPriceBreadth": None,
            "putCallOptions": None,
            "marketVolatility": None,
            "junkBondDemand": None,
            "safeHavenDemand": None,
        },
    )

    return {
        "ok": True,
        "updatedAt": now_iso(),
        "market": market,
        "core": core,
        "fearGreed": fear_greed,
        "meta": {
            "source": "scripts/update_data_mix.py",
            "fredMode": "mixed_sources",
            "trustedPrev": PREV_TRUSTED,
            "errors": errors,
        },
    }

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log("데이터 생성 시작")
    payload = build_payload()

    LATEST_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    STATUS_PATH.write_text(json.dumps({
        "ok": True,
        "updatedAt": payload["updatedAt"],
        "fredMode": payload["meta"]["fredMode"],
        "trustedPrev": payload["meta"]["trustedPrev"],
        "errorCount": len(errors),
        "errors": errors,
    }, ensure_ascii=False, indent=2))

    log("latest.json 생성 완료")
    log("updatedAt:", payload["updatedAt"])
    log("fredMode :", payload["meta"]["fredMode"])
    log("trustedPrev:", payload["meta"]["trustedPrev"])
    log("errors   :", len(errors))

if __name__ == "__main__":
    main()
