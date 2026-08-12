"""Install the OSTN15 datum-shift grid into PROJ's data directory.

Run at image build time, after the virtualenv exists.

PROJ ships no UK grid. Without this file every EPSG:4326 <-> EPSG:27700
conversion silently falls back to a 7-parameter Helmert approximation of the
OSGB36/ETRS89 relationship, which is wrong by roughly 3m across East Anglia.
That is two orders of magnitude larger than the centimetre-scale overlaps
`/check-boundary` resolves against EDP exclusion zones, so without the grid a
boundary drawn within a few metres of a zone edge can be judged eligible when
it is not (or the reverse), and results disagree between environments.

The failure mode is silent -- PROJ reports no error and returns plausible
coordinates -- so this script verifies the download and refuses to install a
grid that is not byte-for-byte what we expect. A build that cannot reach the
CDN fails loudly rather than producing a quietly inaccurate image.

Source: Ordnance Survey OSTN15, redistributed by the PROJ CDN under the Open
Government Licence. proj.db already registers the transform that consumes it,
so placing the file in PROJ's data directory is all that is required; there is
no accompanying configuration to change.
"""

import hashlib
import sys
import urllib.request
from pathlib import Path

import pyproj

GRID_FILENAME = "uk_os_OSTN15_NTv2_OSGBtoETRS.tif"
GRID_URL = f"https://cdn.proj.org/{GRID_FILENAME}"
GRID_SHA256 = "5d6ed64d2119952c4c559fa1fccbc594b6520fc3ec3ef2fc10be13202c4384fa"


def install() -> Path:
    """Download the grid into PROJ's data directory and return its path.

    The destination is resolved from pyproj rather than hardcoded, because the
    path embeds the Python version (``.../python3.14/site-packages/...``) and
    would break on the next interpreter bump.
    """
    with urllib.request.urlopen(GRID_URL) as response:  # noqa: S310 - fixed https URL
        payload = response.read()

    digest = hashlib.sha256(payload).hexdigest()
    if digest != GRID_SHA256:
        message = (
            f"{GRID_FILENAME} failed checksum verification: "
            f"expected {GRID_SHA256}, got {digest}"
        )
        raise SystemExit(message)

    destination = Path(pyproj.datadir.get_data_dir()) / GRID_FILENAME
    destination.write_bytes(payload)
    # Read-only: nothing should ever rewrite the grid in place. This is not a
    # security boundary -- the build runs as the same unprivileged user that
    # owns the virtualenv, so that user can still chmod or replace the file.
    # It guards against accident, not against a determined process.
    destination.chmod(0o444)
    return destination


if __name__ == "__main__":
    path = install()
    print(f"Installed {GRID_FILENAME} ({path.stat().st_size} bytes) to {path}")
    sys.exit(0)
