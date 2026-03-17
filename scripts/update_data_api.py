#!/usr/bin/env python3
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LATEST_PATH = DATA_DIR / "latest.json"
STATUS_PATH = DATA_DIR / "status.json"

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
FRED_API_BASE = "https://api.stlouisfed.org/fred/series/observations"
CNN_FG_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

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
PREV_TRUSTED = PREV.get("meta", {}).get("source") == "scripts/update_data_api.py"

def prev_get(*keys):
    if not PREV_TRUSTED:
        return None
    cur = PREV
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def curl_json(url, timeout=20, extra_headers=None):
    cmd = [
        "curl",
        "-L",
        "--compressed",
        "-sS",
        "--connect-timeout", "8",
        "--max-time", str(timeout),
        "--retry", "0",
        "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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

    try:
        data = json.loads(body)
    except Exception:
        snippet = (body or res.stderr or "")[:400].replace("\n", " ")
        if status:
            raise RuntimeError(f"HTTP {status} 응답 파싱 실패: {snippet}")
        raise RuntimeError(f"JSON 파싱 실패: {snippet}")

    if status and status >= 400:
        msg = data.get("error_message") or data.get("message") or str(data)
        raise RuntimeError(f"HTTP {status}: {msg}")

    return data

def load_fred_series(series_id):
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY 없음")

    qs = urlencode({
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
        "limit": "2000",
    })
    url = f"{FRED_API_BASE}?{qs}"
    data = curl_json(url, timeout=20)

    obs = data.get("observations", [])
    out = []
    for row in obs:
        d = row.get("date")
        v = safe_float(row.get("value"))
        if d and v is not None:
            out.append({"date": d, "value": v})

    if not out:
        raise RuntimeError(f"FRED API 빈 응답: {series_id}")
    return out

def load_first_available(series_ids):
    last_error = None
    for sid in series_ids:
        try:
            return sid, load_fred_series(sid)
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

def build_payload():
    market = {
        "sp": fetch_or_prev(
            "market.sp",
            lambda: transform_value(load_fred_series("SP500")),
            ("market", "sp"),
            {"value": None, "date": None},
        ),
        "nd": fetch_or_prev(
            "market.nd",
            lambda: transform_value(load_first_available(["NASDAQ100", "NASDAQCOM"])[1]),
            ("market", "nd"),
            {"value": None, "date": None},
        ),
        "go": fetch_or_prev(
            "market.go",
            lambda: transform_value(load_first_available(["GOLDAMGBD228NLBM", "GOLDPMGBD228NLBM"])[1]),
            ("market", "go"),
            {"value": None, "date": None},
        ),
        "dx": fetch_or_prev(
            "market.dx",
            lambda: transform_value(load_fred_series("DTWEXBGS")),
            ("market", "dx"),
            {"value": None, "date": None},
        ),
        "vx": fetch_or_prev(
            "market.vx",
            lambda: transform_value(load_fred_series("VIXCLS")),
            ("market", "vx"),
            {"value": None, "date": None},
        ),
    }

    core = {
        "lei": fetch_or_prev(
            "core.lei",
            lambda: transform_value(load_fred_series("USSLIND"), "mom"),
            ("core", "lei"),
            {"value": None, "date": None},
        ),
        "pmi": fetch_or_prev(
            "core.pmi",
            lambda: transform_value(load_fred_series("NAPMPMI")),
            ("core", "pmi"),
            {"value": None, "date": None},
        ),
        "buffett": fetch_or_prev(
            "core.buffett",
            lambda: transform_value(load_fred_series("DDDM01USA156NWDB")),
            ("core", "buffett"),
            {"value": None, "date": None},
        ),
        "jolts": fetch_or_prev(
            "core.jolts",
            lambda: transform_value(load_fred_series("JTSJOL"), "div1000"),
            ("core", "jolts"),
            {"value": None, "date": None},
        ),
        "marginDebt": fetch_or_prev(
            "core.marginDebt",
            lambda: transform_value(load_fred_series("BOGZ1FL663067003Q"), "div1000"),
            ("core", "marginDebt"),
            {"value": None, "date": None},
        ),
        "michigan": fetch_or_prev(
            "core.michigan",
            lambda: transform_value(load_fred_series("UMCSENT")),
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
            "source": "scripts/update_data_api.py",
            "fredMode": "official_api",
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
