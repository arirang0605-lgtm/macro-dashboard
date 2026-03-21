def valuation_score(shiller_cape):

    if shiller_cape < 16:
        return 0.0
    elif shiller_cape < 22:
        return 0.2
    elif shiller_cape < 28:
        return 0.5
    elif shiller_cape < 33:
        return 0.75
    else:
        return 1.0


def fragility_score(
    hy_spread,
    hy_spread_36m_low,
    current_vix,
    vix_36m_avg
):

    spread_compression = (hy_spread - hy_spread_36m_low) / hy_spread_36m_low
    vix_compression = (current_vix - vix_36m_avg) / vix_36m_avg

    if spread_compression < 0.10 and vix_compression < -0.25:
        return 0.8

    elif spread_compression < 0.20 or vix_compression < -0.15:
        return 0.5

    else:
        return 0.2


def bubble_risk(valuation_s, fragility_s):

    return (valuation_s * 0.6) + (fragility_s * 0.4)


FALL_TRIGGER = 0.65


def detect_fall(macro_season, bubble_risk_score):

    if macro_season == "Summer" and bubble_risk_score >= FALL_TRIGGER:
        return "Fall"

    return macro_season


if __name__ == "__main__":

    shiller_cape = 31
    hy_spread = 3.4
    hy_spread_36m_low = 3.2
    current_vix = 13
    vix_36m_avg = 18

    val = valuation_score(shiller_cape)

    frag = fragility_score(
        hy_spread,
        hy_spread_36m_low,
        current_vix,
        vix_36m_avg
    )

    risk = bubble_risk(val, frag)

    print("Valuation Score:", val)
    print("Fragility Score:", frag)
    print("Bubble Risk:", round(risk, 3))
