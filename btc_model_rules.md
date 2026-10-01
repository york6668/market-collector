version: 1.0

trend:

  UPTREND:
    price_ma: above
    oi: up
    volume: up

  DOWNTREND:
    price_ma: below
    oi: up
    volume: up

  NEUTRAL:
    default: true


volatility:

  LOW_VOL:
    atr: "<0.02"

  MID_VOL:
    atr: "0.02-0.04"

  HIGH_VOL:
    atr: ">0.04"


liquidity:

  EXPANDING:
    oi: up
    volume: up

  DRY:
    oi: down
    volume: down

  DELEVERAGING:
    oi: down
    price: down


trend_quality:

  STRONG:
    price: up
    oi: up
    volume: up

  WEAK:
    price: up
    oi: down

  REVERSAL:
    price: down
    oi: up


risk:

  HIGH:
    volatility: HIGH_VOL

  MID:
    trend: NEUTRAL

  LOW:
    default: true


execution:

  EXECUTE:
    trend: UPTREND
    tq: STRONG
    liquidity: EXPANDING

  WAIT:
    trend: NEUTRAL

  AVOID:
    risk: HIGH