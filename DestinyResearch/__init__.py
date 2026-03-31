# DestinyResearch/__init__.py
# Expose all public functions from dr.py at the package level.
# This allows both:
#     import DestinyResearch as dr  →  dr.get_mbo(...)
#     from DestinyResearch import get_mbo, show_info

from DestinyResearch.dr import *  # noqa: F401, F403
from DestinyResearch import dr    # noqa: F401  (also expose the submodule itself)