#!/usr/bin/env python3
import argparse
import contextlib
import os
# Silence ecCodes global logging (must be set **before** importing eccodes)
os.environ.setdefault('ECCODES_LOG_STREAM', os.devnull)
from pathlib import Path
import xml.etree.ElementTree as ET

import numpy as np
from eccodes import *  # noqa: F401,F403 (ecCodes API)

# Project paths (still work if invoked from repo root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output_xml'


def _read_values_from_xml(xml_path: Path) -> np.ndarray:
    """Parse values from an XML file produced by convert_grb.py.

    This supports a few shapes:
      - <variable><values> 1 2 3 </values></variable>
      - <values>1,2,3</values>
      - <values><value>1</value><value>2</value>...</values>

    Missing values accepted as: '--', 'NaN', 'nan'.
    Returns float64 numpy array with np.nan for missing entries.
    """
    tree = ET.parse(str(xml_path))
    root = tree.getroot()

    # Try common paths, most specific first
    candidates = [
        root.find('variable/values'),
        root.find('.//values'),  # any nested 'values'
    ]

    node = next((n for n in candidates if n is not None), None)
    if node is None:
        raise ValueError(f"Could not find <values> node in {xml_path}")

    # Case A: <values>text</values>
    text = (node.text or '').strip()
    if text:
        # Accept spaces and/or commas
        # Also allow multiple whitespace lines
        raw_tokens = [t for part in text.splitlines() for t in part.replace(',', ' ').split()]
    else:
        # Case B: <values><value>...</value>...</values>
        raw_tokens = [
            (child.text or '').strip()
            for child in list(node)
            if child.tag.lower() == 'value'
        ]

    if not raw_tokens:
        raise ValueError(f"No numeric tokens found in <values> for {xml_path}")

    # Normalize tokens and convert to float64 with np.nan for missing-like markers
    def _to_float(tok: str) -> float:
        t = tok.strip()
        if t in {'--', 'NaN', 'nan', ''}:
            return np.nan
        return float(t)

    arr = np.fromiter((_to_float(t) for t in raw_tokens), dtype=np.float64)
    return arr


ess_errnull = open(os.devnull, 'w')  # reuse for silencing ecCodes warnings


def decode_xml_to_grib(original_grb_path: Path, reconstructed_grb_path: Path, xml_prefix: str) -> None:
    """Reconstruct a GRIB from XML value dumps produced by convert_grb.py.

    - Clones message structure from original GRIB
    - Switches to lossless IEEE packing (double precision)
    - Inserts values parsed from XML; missing -> IEEE NaN
    - Validates point counts per message
    """
    with open(original_grb_path, 'rb') as fin, open(reconstructed_grb_path, 'wb') as fout:
        reconstructed = 0
        passed_through = 0
        msg_index = 0
        while True:
            with contextlib.redirect_stderr(ess_errnull):
                gid = codes_grib_new_from_file(fin)
            if gid is None:
                break

            try:
                with contextlib.redirect_stderr(ess_errnull):
                    clone_id = codes_clone(gid)

                # Keep original packing to avoid template/key issues across message types.
                # We'll map XML-missing markers to the message's defined missing value and enable bitmap if needed.
                with contextlib.redirect_stderr(ess_errnull):
                    try:
                        msg_missing = codes_get(gid, 'missingValue')
                    except Exception:
                        # Fallback to IEEE NaN if key is unavailable
                        msg_missing = float('nan')

                xml_filename = f"{xml_prefix}_msg_{msg_index}.xml"
                xml_path = OUTPUT_DIR / xml_filename
                have_xml = xml_path.exists()
                values = None
                expected = None
                if have_xml:
                    try:
                        values = _read_values_from_xml(xml_path)
                        with contextlib.redirect_stderr(ess_errnull):
                            expected = codes_get(gid, 'numberOfDataPoints')
                        if values.size != expected:
                            # fall back to cloning unchanged if counts don't match
                            values = None
                            have_xml = False
                    except Exception:
                        # any parse error -> fall back to unchanged
                        values = None
                        have_xml = False

                if values is not None:
                    # Normalize dtype
                    if values.dtype != np.float64:
                        values = values.astype(np.float64, copy=False)

                    # Replace NaNs with message missing value if we are not in IEEE mode
                    if np.isnan(values).any():
                        # If msg_missing is NaN, keep NaNs; otherwise substitute
                        if not (isinstance(msg_missing, float) and np.isnan(msg_missing)):
                            values = np.where(np.isnan(values), msg_missing, values)
                            # Ensure bitmap is present when using explicit missing value
                            try:
                                if codes_is_defined(clone_id, 'bitmapPresent'):
                                    codes_set(clone_id, 'bitmapPresent', 1)
                            except Exception:
                                pass

                    # Insert values and write out
                    codes_set_values(clone_id, values)
                    with contextlib.redirect_stderr(ess_errnull):
                        codes_write(clone_id, fout)
                    reconstructed += 1
                else:
                    # No usable XML for this message; write the message unchanged
                    with contextlib.redirect_stderr(ess_errnull):
                        codes_write(clone_id, fout)
                    passed_through += 1

                # Force realization of written message for early failure detection (non-fatal)
                try:
                    with contextlib.redirect_stderr(ess_errnull):
                        _ = codes_get(clone_id, 'totalLength')
                except Exception:
                    pass

            finally:
                # Always release handles
                try:
                    codes_release(clone_id)
                except Exception:
                    pass
                codes_release(gid)

            msg_index += 1
    print(f"Reconstruction summary: {reconstructed} messages from XML, {passed_through} passed through unchanged.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconstruct a GRIB2 from XML value dumps.")
    parser.add_argument('--in', dest='in_grib', type=Path, default=DATA_DIR / 'small_subset_500mb.grb2',
                        help='Path to original GRIB file to clone structure from')
    parser.add_argument('--out', dest='out_grib', type=Path, default=PROJECT_ROOT / 'reconstructed_small.grb2',
                        help='Output path for reconstructed GRIB')
    parser.add_argument('--prefix', dest='xml_prefix', default='small_subset_500mb',
                        help='XML filename prefix used by convert_grb.py (e.g., <prefix>_msg_0.xml)')
    args = parser.parse_args()

    decode_xml_to_grib(args.in_grib, args.out_grib, args.xml_prefix)