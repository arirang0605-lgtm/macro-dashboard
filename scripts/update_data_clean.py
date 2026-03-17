#!/usr/bin/env python3
import csv
import io
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LATEST_PATH = DATA_DIR / "latest.json"
STATUS_PATH = DATA_DIR / "status.json"

FRED_CSV_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
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

def prev_get(*keys):
    cur = PREV
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def curl_text(url, timeout=40, extra_headers=None, http1=False):
    cmd = [
        "curl",
        "-L",
        "--compressed",
        "-sS",
        "--fail",
        "--connect-timeout", "15",
        "--max-time", str(timeout),
        "--retry", "2",
        "--retry-all-errors",
        "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ]

    if http1:
        cmd.append("--http1.1")

    headers = extra_headers or []
    for h in headers:
        cmd += ["-H", h]

    cmd.append(url)

    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip() or f"curl 실패: {url}")
    return res.stdout

def load_fred_series_from_csv(series_id):
    text = curl_text(
        FRED_CSV_BASE + quote(series_id),
        timeout=45,
        http1=True,
    )
    reader = csv.DictReader(io.StringIO(text))
    out = []
    for row in reader:
        d = row.get("DATE") or row.get("date")
        v = safe_float(row.get(series_id) or row.get("VALUE") or row.get("value"))
        if d and v is not None:
            out.append({"date": d, "value": v})

    if not out:
        raise RuntimeError(f"FRED CSV 빈 응답: {series_id}")
    return out

def load_first_available(series_ids):
    last_error = None
    for sid in series_ids:
        try:
            return sid, load_fred_series_from_csv(sid)
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
    text = curl_text(
        CNN_FG_URL,
        timeout=30,
        extra_headers=[
            "accept: application/json, text/plain, */*",
            "referer: https://www.cnn.com/markets/fear-and-greed",
            "origin: https://www.cnn.com",
        ],
    )
    data = json.loads(text)
    fg = data.get("fear_and_greed") or data

    prev_fg = prev_get("fearGreed") if isinstance(prev_get("fearGreed"), dict) else {}

    return {
        "score": fg.get("score"),
        "rating": fg.get("rating"),
        "timestamp": fg.get("timestamp"),
        "date": fg.get("timestamp"),
        "previousWeek": prev_fg.get("previousWeek"),
        "previousMonth": prev_fg.get("previousMonth"),
        "marketMomentum": prev_fg.get("marketMomentum"),
        "stockPriceStrength": prev_fg.get("stockPriceStrength"),
        "stockPriceBreadth": prev_fg.get("stockPriceBreadth"),
        "putCallOptions": prev_fg.get("putCallOptions"),
        "marketVolatility": prev_fg.get("marketVolatility"),
        "junkBondDemand": prev_fg.get("junkBondDemand"),
        "safeHavenDemand": prev_fg.get("safeHavenDemand"),
    }

def build_payload():
    market = {
        "sp": fetch_or_prev(
            "market.sp",
            lambda: transform_value(load_fred_series_from_csv("SP500")),
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
            lambda: transform_value(load_fred_series_from_csv("DTWEXBGS")),
            ("market", "dx"),
            {"value": None, "date": None},
        ),
        "vx": fetch_or_prev(
            "market.vx",
            lambda: transform_value(load_fred_series_from_csv("VIXCLS")),
            ("market", "vx"),
            {"value": None, "date": None},
        ),
    }

    core = {
        "lei": fetch_or_prev(
            "core.lei",
            lambda: transform_value(load_fred_series_from_csv("USSLIND"), "mom"),
            ("core", "lei"),
            {"value": None, "date": None},
        ),
        "pmi": fetch_or_prev(
            "core.pmi",
            lambda: transform_value(load_fred_series_from_csv("NAPMPMI")),
            ("core", "pmi"),
            {"value": None, "date": None},
        ),
        "buffett": fetch_or_prev(
            "core.buffett",
            lambda: transform_value(load_fred_series_from_csv("DDDM01USA156NWDB")),
            ("core", "buffett"),
            {"value": None, "date": None},
        ),
        "jolts": fetch_or_prev(
            "core.jolts",
            lambda: transform_value(load_fred_series_from_csv("JTSJOL"), "div1000"),
            ("core", "jolts"),
            {"value": None, "date": None},
        ),
        "marginDebt": fetch_or_prev(
            "core.marginDebt",
            lambda: transform_value(load_fred_series_from_csv("BOGZ1FL663067003Q"), "div1000"),
            ("core", "marginDebt"),
            {"value": None, "date": None},
        ),
        "michigan": fetch_or_prev(
            "core.michigan",
            lambda: transform_value(load_fred_series_from_csv("UMCSENT")),
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
            "source": "scripts/update_data_clean.py",
            "fredMode": "csv_http1",
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
        "errorCount": len(errors),
        "errors": errors,
    }, ensure_ascii=False, indent=2))

    log("latest.json 생성 완료")
    log("updatedAt:", payload["updatedAt"])
    log("fredMode :", payload["meta"]["fredMode"])
    log("errors   :", len(errors))

if __name__ == "__main__":
    main()
