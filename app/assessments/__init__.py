"""Impact assessment implementations.

Each module in this package implements a specific environmental impact
assessment (nutrients, GCN, DLL, etc.) following the pattern:
- Constructor: __init__(rlb_gdf, metadata, repository)
- Run method: run() -> dict[str, DataFrame]
"""
