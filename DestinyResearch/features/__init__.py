# DestinyResearch.features — signal engineering modules
#
# Atomic features (single metric, MBO or MBP-1 source):
#   spread.py  — bid-ask spread (MBP-1)
#   volume.py  — trade volume (MBO)
#   depth.py   — best bid/ask depth (MBP-1)
#   otr.py     — Order-to-Trade Ratio (MBO)
#
# Composite analyses (orchestrate atomic features):
#   descriptive/ — rolling metrics, intraday seasonality
#
# Planned:
#   ofi/      — Order Flow Imbalance (Cont et al. 2014)
#   vpin/     — Volume-Synchronized Probability of Informed Trading
#   lead_lag/ — cross-instrument price discovery