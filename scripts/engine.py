import json
from pathlib import Path

from bubble_engine import valuation_score, fragility_score, bubble_risk, detect_fall

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

LATEST_FILE = DATA_DIR / "latest.json"
HISTORY_FILE = DATA_DIR / "history.json"
STATE_FILE = DATA_DIR / "persistence_state.json"

LEVEL_WEIGHT = 0.7
TREND_WEIGHT = 0.3

AXIS_WEIGHTS = {
    "credit": 0.30,
    "employment": 0.30,
    "leading": 0.25,
    "policy": 0.15,
}

# 높은 점수 = 더 건강한 상태
PERSISTENCE = {
    "credit": {
        "deteriorate": 0,
        "recover": 1,
    },
    "employment": {
        "deteriorate": 2,
        "recover": 4,
    },
    "leading": {
        "deteriorate": 2,
        "recover": 5,
    },
    "policy": {
        "deteriorate": 0,
        "recover": 1,
    },
}


def load_json(path):
    with open(path) as f:
        return json.load(f)


def load_latest():
    return load_json(LATEST_FILE)


def load_history():
    return load_json(HISTORY_FILE)


def load_state():
    if not STATE_FILE.exists():
        return {}
    return load_json(STATE_FILE)


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def max_date(*dates):
    valid = [d for d in dates if d]
    if not valid:
        return ""
    return max(valid)


# -----------------------------
# LEVEL SCORE
# -----------------------------

def score_hy_spread(v):
    if v < 3:
        return 0.9
    elif v < 4:
        return 0.7
    elif v < 6:
        return 0.4
    else:
        return 0.1


def score_icsa(v):
    if v < 220000:
        return 0.8
    elif v < 260000:
        return 0.6
    elif v < 300000:
        return 0.4
    else:
        return 0.2


def score_continuing_claims(v):
    if v < 1850000:
        return 0.8
    elif v < 2100000:
        return 0.6
    elif v < 2400000:
        return 0.4
    else:
        return 0.2


def score_bbb_spread(v):
    if v < 1.5:
        return 0.9
    elif v < 2.0:
        return 0.7
    elif v < 3.0:
        return 0.4
    else:
        return 0.1


def score_sahm(v):
    if v < 0.3:
        return 0.8
    elif v < 0.5:
        return 0.6
    elif v < 1.0:
        return 0.3
    else:
        return 0.1


def score_pmi(v):
    if v is None:
        return 0.5
    if v > 55:
        return 0.9
    elif v > 50:
        return 0.7
    elif v > 45:
        return 0.4
    else:
        return 0.2


def score_lei(v):
    if v > 0.3:
        return 0.8
    elif v > 0:
        return 0.6
    elif v > -0.5:
        return 0.4
    else:
        return 0.2


def score_yield_curve(v):
    if v > 0.5:
        return 0.8
    elif v > 0:
        return 0.6
    elif v > -0.5:
        return 0.4
    else:
        return 0.2


# -----------------------------
# TREND SCORE
# history 배열: [현재, 1개월전, 2개월전]
# -----------------------------

def trend_score_positive(series):
    """
    값이 올라갈수록 좋은 지표
    예: PMI, LEI
    """
    if not series or len(series) < 3:
        return 0.5

    current = series[0]
    old = series[-1]
    delta = current - old

    if delta > 1.0:
        return 0.8
    elif delta > 0.2:
        return 0.65
    elif delta > -0.2:
        return 0.5
    elif delta > -1.0:
        return 0.35
    else:
        return 0.2


def trend_score_negative(series):
    """
    값이 내려갈수록 좋은 지표
    예: ICSA
    """
    if not series or len(series) < 3:
        return 0.5

    current = series[0]
    old = series[-1]
    delta = current - old

    if delta < -10000:
        return 0.8
    elif delta < -2000:
        return 0.65
    elif delta < 2000:
        return 0.5
    elif delta < 10000:
        return 0.35
    else:
        return 0.2


def trend_score_negative_small(series):
    """
    값이 내려갈수록 좋은데 절대 변화폭이 작은 지표
    예: HY Spread
    """
    if not series or len(series) < 3:
        return 0.5

    current = series[0]
    old = series[-1]
    delta = current - old

    if delta < -0.20:
        return 0.8
    elif delta < -0.05:
        return 0.65
    elif delta < 0.05:
        return 0.5
    elif delta < 0.20:
        return 0.35
    else:
        return 0.2


def combine_score(level_score, trend_score):
    return (level_score * LEVEL_WEIGHT) + (trend_score * TREND_WEIGHT)


# -----------------------------
# AXIS CALC
# -----------------------------

def credit_axis(core, history):
    hy_level = score_hy_spread(core["hySpread"]["value"])
    hy_trend = trend_score_negative_small(history["hySpread"])
    hy_final = combine_score(hy_level, hy_trend)

    bbb_level = score_bbb_spread(core["bbbSpread"]["value"])
    bbb_hist = history.get("bbbSpread")
    bbb_trend = trend_score_negative_small(bbb_hist) if bbb_hist else 0.5
    bbb_final = combine_score(bbb_level, bbb_trend)

    final = (hy_final + bbb_final) / 2

    return {
        "hy_level": hy_level,
        "hy_trend": hy_trend,
        "hy_final": hy_final,
        "bbb_level": bbb_level,
        "bbb_trend": bbb_trend,
        "bbb_final": bbb_final,
        "raw_final": final,
        "stamp": max_date(core["hySpread"]["date"], core["bbbSpread"]["date"]),
    }


def employment_axis(core, history):
    icsa_level = score_icsa(core["icsa"]["value"])
    icsa_trend = trend_score_negative(history["icsa"])
    icsa_final = combine_score(icsa_level, icsa_trend)

    cc_level = score_continuing_claims(core["continuingClaims"]["value"])
    cc_hist = history.get("continuingClaims")
    cc_trend = trend_score_negative(cc_hist) if cc_hist else 0.5
    cc_final = combine_score(cc_level, cc_trend)

    sahm_level = score_sahm(core["sahm"]["value"])
    sahm_final = sahm_level

    final = (icsa_final + cc_final + sahm_final) / 3

    return {
        "icsa_level": icsa_level,
        "icsa_trend": icsa_trend,
        "icsa_final": icsa_final,
        "continuing_claims_level": cc_level,
        "continuing_claims_trend": cc_trend,
        "continuing_claims_final": cc_final,
        "sahm_level": sahm_level,
        "raw_final": final,
        "stamp": max_date(max_date(core["icsa"]["date"], core["continuingClaims"]["date"]), core["sahm"]["date"]),
    }


def leading_axis(core, history):
    pmi_level = score_pmi(core["pmi"]["value"])
    pmi_trend = trend_score_positive(history["pmi"])
    pmi_final = combine_score(pmi_level, pmi_trend)

    spmi_value = core.get("servicesPmi", {}).get("value")
    if spmi_value is None:
        spmi_level = 0.5
        spmi_trend = 0.5
        spmi_final = 0.5
    else:
        spmi_level = score_pmi(spmi_value)
        spmi_hist = history.get("servicesPmi")
        spmi_trend = trend_score_positive(spmi_hist) if spmi_hist else 0.5
        spmi_final = combine_score(spmi_level, spmi_trend)

    lei_level = score_lei(core["lei"]["value"])
    lei_trend = trend_score_positive(history["lei"])
    lei_final = combine_score(lei_level, lei_trend)

    final = (pmi_final + spmi_final + lei_final) / 3

    return {
        "pmi_level": pmi_level,
        "pmi_trend": pmi_trend,
        "pmi_final": pmi_final,
        "services_pmi_level": spmi_level,
        "services_pmi_trend": spmi_trend,
        "services_pmi_final": spmi_final,
        "lei_level": lei_level,
        "lei_trend": lei_trend,
        "lei_final": lei_final,
        "raw_final": final,
        "stamp": max_date(max_date(core["pmi"]["date"], core["servicesPmi"]["date"]), core["lei"]["date"]),
    }


def score_fedfunds(v):
    if v is None:
        return 0.5
    if v >= 5.0:
        return 0.2
    elif v >= 4.0:
        return 0.35
    elif v >= 3.0:
        return 0.5
    elif v >= 2.0:
        return 0.65
    else:
        return 0.8


def policy_axis(core):
    yc_level = score_yield_curve(core["t10y2y"]["value"])
    ff_level = score_fedfunds(core.get("fedfunds", {}).get("value"))
    final = (yc_level + ff_level) / 2

    return {
        "yc_level": yc_level,
        "fedfunds_level": ff_level,
        "raw_final": final,
        "stamp": max_date(core["t10y2y"]["date"], core["fedfunds"]["date"]),
    }


# -----------------------------
# PERSISTENCE
# -----------------------------

def init_axis_state(raw_score, stamp):
    return {
        "effective_score": raw_score,
        "last_raw_score": raw_score,
        "last_stamp": stamp,
        "pending_direction": None,
        "pending_count": 0,
    }


def apply_axis_persistence(axis_name, raw_score, stamp, state):
    axis_state = state.get(axis_name)

    if not axis_state:
        axis_state = init_axis_state(raw_score, stamp)
        state[axis_name] = axis_state
        return raw_score, {
            "status": "initialized",
            "direction": "flat",
            "required": 0,
            "pending_count": 0,
        }

    effective_score = axis_state["effective_score"]
    prev_stamp = axis_state.get("last_stamp", "")

    if stamp == prev_stamp:
        axis_state["last_raw_score"] = raw_score
        state[axis_name] = axis_state
        return effective_score, {
            "status": "same_stamp_hold",
            "direction": "flat",
            "required": 0,
            "pending_count": axis_state.get("pending_count", 0),
        }

    eps = 1e-9

    if raw_score > effective_score + eps:
        direction = "recover"
    elif raw_score < effective_score - eps:
        direction = "deteriorate"
    else:
        direction = "flat"

    if direction == "flat":
        axis_state["effective_score"] = raw_score
        axis_state["last_raw_score"] = raw_score
        axis_state["last_stamp"] = stamp
        axis_state["pending_direction"] = None
        axis_state["pending_count"] = 0
        state[axis_name] = axis_state

        return raw_score, {
            "status": "flat_update",
            "direction": "flat",
            "required": 0,
            "pending_count": 0,
        }

    required = PERSISTENCE[axis_name][direction]

    if required == 0:
        axis_state["effective_score"] = raw_score
        axis_state["last_raw_score"] = raw_score
        axis_state["last_stamp"] = stamp
        axis_state["pending_direction"] = None
        axis_state["pending_count"] = 0
        state[axis_name] = axis_state

        return raw_score, {
            "status": "immediate_apply",
            "direction": direction,
            "required": required,
            "pending_count": 0,
        }

    prev_pending_direction = axis_state.get("pending_direction")
    prev_pending_count = axis_state.get("pending_count", 0)

    if prev_pending_direction == direction:
        pending_count = prev_pending_count + 1
    else:
        pending_count = 1

    if pending_count >= required:
        axis_state["effective_score"] = raw_score
        axis_state["last_raw_score"] = raw_score
        axis_state["last_stamp"] = stamp
        axis_state["pending_direction"] = None
        axis_state["pending_count"] = 0
        state[axis_name] = axis_state

        return raw_score, {
            "status": "confirmed_apply",
            "direction": direction,
            "required": required,
            "pending_count": pending_count,
        }

    axis_state["last_raw_score"] = raw_score
    axis_state["last_stamp"] = stamp
    axis_state["pending_direction"] = direction
    axis_state["pending_count"] = pending_count
    state[axis_name] = axis_state

    return effective_score, {
        "status": "hold_waiting_confirmation",
        "direction": direction,
        "required": required,
        "pending_count": pending_count,
    }


# -----------------------------
# SEASON / STAGE
# Fall은 bubble overlay로만 진입
# -----------------------------

def classify_base_season(score):
    if score < 0.35:
        return "겨울 (Recession)"
    elif score < 0.65:
        return "봄 (Recovery)"
    else:
        return "여름 (Expansion)"


def classify_stage(score, season=None, delta=0.0):
    if score < 0.25:
        band_pos = score / 0.25
    elif score < 0.50:
        band_pos = (score - 0.25) / 0.25
    elif score < 0.75:
        band_pos = (score - 0.50) / 0.25
    else:
        band_pos = (score - 0.75) / 0.25

    # 현재 raw score가 effective score보다 얼마나 앞서/뒤처지는지 약하게 반영
    # 겨울은 점수 상승이 "덜 나빠짐"이므로 방향을 반대로 해석
    if delta >= 0.03:
        band_pos += -0.12 if season == "겨울 (Recession)" else 0.12
    elif delta <= -0.03:
        band_pos += 0.12 if season == "겨울 (Recession)" else -0.12

    band_pos = max(0.0, min(0.999, band_pos))

    if band_pos < 0.25:
        return "L1"
    elif band_pos < 0.50:
        return "L2"
    elif band_pos < 0.75:
        return "L3"
    else:
        return "L4"


# -----------------------------
# BUBBLE ENGINE INPUTS
# 지금은 latest.json 구조에 맞춰 임시 연결
# -----------------------------

def run_bubble_overlay(latest, history):
    buffett_proxy = latest["core"].get("buffett", {}).get("value", 180)
    hy_spread = latest["core"]["hySpread"]["value"]

    hy_hist = [
        v for v in (history.get("hySpread") or [])
        if isinstance(v, (int, float))
    ]
    if isinstance(hy_spread, (int, float)):
        hy_hist.append(hy_spread)

    hy_spread_36m_low = min(hy_hist) if hy_hist else 3.0
    if not hy_spread_36m_low or hy_spread_36m_low <= 0:
        hy_spread_36m_low = 3.0

    current_vix = latest["market"]["vx"]["value"]

    vx_hist = [
        v for v in (history.get("vx") or [])
        if isinstance(v, (int, float))
    ]
    vix_36m_avg = round(sum(vx_hist) / len(vx_hist), 2) if vx_hist else 18.0

    val = valuation_score(buffett_proxy)
    frag = fragility_score(
        hy_spread,
        hy_spread_36m_low,
        current_vix,
        vix_36m_avg
    )
    risk = bubble_risk(val, frag)

    return {
        "valuation": val,
        "fragility": frag,
        "risk": risk,
        "hy_spread_36m_low": hy_spread_36m_low,
        "vix_36m_avg": vix_36m_avg,
    }


# -----------------------------
# MAIN ENGINE
# -----------------------------

def run_engine():
    latest = load_latest()
    history = load_history()
    state = load_state()
    core = latest["core"]

    credit = credit_axis(core, history)
    employment = employment_axis(core, history)
    leading = leading_axis(core, history)
    policy = policy_axis(core)

    raw_macro_score = (
        credit["raw_final"] * AXIS_WEIGHTS["credit"] +
        employment["raw_final"] * AXIS_WEIGHTS["employment"] +
        leading["raw_final"] * AXIS_WEIGHTS["leading"] +
        policy["raw_final"] * AXIS_WEIGHTS["policy"]
    )

    credit["effective_final"], credit["persistence"] = apply_axis_persistence(
        "credit", credit["raw_final"], credit["stamp"], state
    )
    employment["effective_final"], employment["persistence"] = apply_axis_persistence(
        "employment", employment["raw_final"], employment["stamp"], state
    )
    leading["effective_final"], leading["persistence"] = apply_axis_persistence(
        "leading", leading["raw_final"], leading["stamp"], state
    )
    policy["effective_final"], policy["persistence"] = apply_axis_persistence(
        "policy", policy["raw_final"], policy["stamp"], state
    )

    save_state(state)

    macro_score = (
        credit["effective_final"] * AXIS_WEIGHTS["credit"] +
        employment["effective_final"] * AXIS_WEIGHTS["employment"] +
        leading["effective_final"] * AXIS_WEIGHTS["leading"] +
        policy["effective_final"] * AXIS_WEIGHTS["policy"]
    )

    raw_base_season = classify_base_season(raw_macro_score)
    base_season = classify_base_season(macro_score)
    stage_delta = raw_macro_score - macro_score

    if stage_delta >= 0.03:
        stage_bias = "warming"
    elif stage_delta <= -0.03:
        stage_bias = "cooling"
    else:
        stage_bias = "flat"

    stage = classify_stage(macro_score, season=base_season, delta=stage_delta)

    bubble = run_bubble_overlay(latest, history)

    final_season = base_season
    if base_season == "여름 (Expansion)":
        mapped = detect_fall("Summer", bubble["risk"])
        if mapped == "Fall":
            final_season = "가을 (Bubble / Late Cycle)"

    return {
        "credit": credit,
        "employment": employment,
        "leading": leading,
        "policy": policy,
        "raw_macro_score": raw_macro_score,
        "macro_score": macro_score,
        "raw_base_season": raw_base_season,
        "base_season": base_season,
        "final_season": final_season,
        "stage": stage,
        "stage_delta": stage_delta,
        "stage_bias": stage_bias,
        "bubble": bubble,
    }


if __name__ == "__main__":
    result = run_engine()

    print("Credit Axis:")
    print("  - Raw:", round(result["credit"]["raw_final"], 3))
    print("  - Effective:", round(result["credit"]["effective_final"], 3))
    print("  - Persistence:", result["credit"]["persistence"])

    print("Employment Axis:")
    print("  - Raw:", round(result["employment"]["raw_final"], 3))
    print("  - Effective:", round(result["employment"]["effective_final"], 3))
    print("  - Persistence:", result["employment"]["persistence"])

    print("Leading Axis:")
    print("  - Raw:", round(result["leading"]["raw_final"], 3))
    print("  - Effective:", round(result["leading"]["effective_final"], 3))
    print("  - Persistence:", result["leading"]["persistence"])

    print("Policy Axis:")
    print("  - Raw:", round(result["policy"]["raw_final"], 3))
    print("  - Effective:", round(result["policy"]["effective_final"], 3))
    print("  - Persistence:", result["policy"]["persistence"])

    print("Raw Macro Score:", round(result["raw_macro_score"], 3))
    print("Effective Macro Score:", round(result["macro_score"], 3))
    print("Raw Base Season:", result["raw_base_season"])
    print("Base Season:", result["base_season"])
    print("Final Season:", result["final_season"])
    print("Stage:", result["stage"])
    print("Bubble Risk:", round(result["bubble"]["risk"], 3))
