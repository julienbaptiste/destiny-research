"""Reference cases for the pre-R0 safety net.

These cases intentionally mirror the existing normalization/reconstruction
golden dates. They are centralized here for the new deep-fingerprint layer so
capture/check scripts cannot silently diverge from one another.
"""

from __future__ import annotations


NORMALIZATION_CASES: dict[str, tuple[str, str]] = {
    "ES": ("ESZ25", "2025-10-01"),
    "NIY": ("NIYU25", "2025-06-16"),
    "NKD": ("NKDU25", "2025-06-16"),
    "FDAX": ("FDAXM25", "2025-05-02"),
    "FESX": ("FESXM25", "2025-05-02"),
    "FSMI": ("FSMIM25", "2025-05-02"),
    "HHI": ("HHIG26", "2026-02-03"),
    "HSI": ("HSIG26", "2026-02-03"),
    "MCH": ("MCHG26", "2026-02-03"),
    "MHI": ("MHIG26", "2026-02-03"),
}


RECONSTRUCTION_CASES: dict[str, tuple[str, str]] = {
    "ES": ("ESZ25", "2025-10-01"),
    "NIY": ("NIYU25", "2025-06-16"),
    "NKD": ("NKDU25", "2025-06-16"),
    "FDAX": ("FDAXM25", "2025-05-14"),
    "FESX": ("FESXM25", "2025-05-14"),
    "FSMI": ("FSMIM25", "2025-05-14"),
    "HHI": ("HHIG26", "2026-02-03"),
    "HSI": ("HSIG26", "2026-02-03"),
    "MCH": ("MCHG26", "2026-02-03"),
    "MHI": ("MHIG26", "2026-02-03"),
}
