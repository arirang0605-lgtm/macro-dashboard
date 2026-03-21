import json
from pathlib import Path


DATA_FILE = Path("../data/latest.json")


def load_data():
    with open(DATA_FILE) as f:
        return json.load(f)


# -----------------------------
# LEVEL SCORE FUNCTIONS
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


def score_sahm(v):
    if v < 0.3:
        return 0.8
    elif v < 0.5:
        return 0.6
    elif v < 1:
        return 0.3
    else:
        return 0.1


def score_pmi(v):
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
# AXIS CALCULATION
# -----------------------------

def credit_axis(core):
    return score_hy_spread(core["hySpread"]["value"])


def employment_axis(core):
    icsa = score_icsa(core["icsa"]["value"])
    sahm = score_sahm(core["sahm"]["value"])

    return (icsa + sahm) / 2


def leading_axis(core):
    pmi = score_pmi(core["pmi"]["value"])
    lei = score_lei(core["lei"]["value"])

    return (pmi + lei) / 2


def policy_axis(core):
    yc = score_yield_curve(core["t10y2y"]["value"])
    return yc


# -----------------------------
# SEASON CLASSIFIER
# -----------------------------

def classify_season(score):

    if score > 0.75:
        return "여름 (Expansion)"
    elif score > 0.55:
        return "봄 (Recovery)"
    elif score > 0.35:
        return "가을 (Late Cycle)"
    else:
        return "겨울 (Recession)"


# -----------------------------
# ENGINE
# -----------------------------

def run_engine():

    data = load_data()
    core = data["core"]

    credit = credit_axis(core)
    employment = employment_axis(core)
    leading = leading_axis(core)
    policy = policy_axis(core)

    macro_score = (
        credit * 0.30 +
        employment * 0.30 +
        leading * 0.25 +
        policy * 0.15
    )

    season = classify_season(macro_score)

    return {
        "credit": credit,
        "employment": employment,
        "leading": leading,
        "policy": policy,
        "macro_score": macro_score,
        "season": season
    }


# -----------------------------
# RUN
# -----------------------------

if __name__ == "__main__":

    result = run_engine()

    print("Credit Axis:", round(result["credit"], 3))
    print("Employment Axis:", round(result["employment"], 3))
    print("Leading Axis:", round(result["leading"], 3))
    print("Policy Axis:", round(result["policy"], 3))

    print("Macro Score:", round(result["macro_score"], 3))
    print("Season:", result["season"])
