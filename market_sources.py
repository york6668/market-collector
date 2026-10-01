"""Public protocol identities shared by collectors and offline evidence readers."""
SPOT = "https://data-api.binance.vision/api/v3/klines"
FUTURES = "https://fapi.binance.com/fapi/v1/klines"
OI = "https://fapi.binance.com/futures/data/openInterestHist"
FUNDING = "https://fapi.binance.com/fapi/v1/fundingRate"
ARCHIVE = "https://data.binance.vision/data"
STABLE = "https://stablecoins.llama.fi/stablecoincharts/all"
VIX = "https://cdn-api.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
VXN = "https://fred.stlouisfed.org/graph/fredgraph.csv"
CFTC = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
HL = "https://api.hyperliquid.xyz/info"
STOCK_WS = "wss://nbstream.binance.com/equity"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"


def provenance(endpoint, unit, **extra):
    return {"endpoint_url": endpoint, "access_scope": "public", "unit": unit, **extra}
