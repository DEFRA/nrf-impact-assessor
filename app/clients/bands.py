"""Static nutrient concentration band lookup.

Band 1: <= 0.25 mg/I TP
Band 2: 0.26 - 2 mg/I TP
Band 3: 2.1 - 5 mg/I TP
Band 4: >= 5.1 mg/I TP
"""

# Upper-inclusive thresholds: (upper_bound, band). None = unbounded.
_BAND_THRESHOLDS: list[tuple[float | None, int]] = [
    (0.25, 1),
    (2.0, 2),
    (5.0, 3),
    (None, 4),
]


def get_band(amount: float) -> int:
    """Return the band (1-4) for a nutrient concentration in mg/I TP."""
    for upper, band in _BAND_THRESHOLDS:
        if upper is None or amount <= upper:
            return band
    return _BAND_THRESHOLDS[-1][1]
