#!/usr/bin/env python3
import argparse
import contextlib
import os
from pathlib import Path

# Silence ecCodes logging before import
os.environ.setdefault('ECCODES_LOG_STREAM', os.devnull)

import numpy as np
from eccodes import *  # noqa: F401,F403

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output_xml'

OUTPUT_DIR.mkdir(exist_ok=True)


def dump_grib_to_xml(in_grib: Path, outdir: Path, prefix: str) -> int:
    """Dump all GRIB messages to XML with values in GRIB order.

    Each message becomes: <prefix>_msg_<index>.xml with:
      <variable index="i">
        <values>v1 v2 ...</values>
      </variable>
    Missing values are emitted as '--'. Floats use .17g precision.
    Returns the number of files written.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(in_grib, 'rb') as fin:
        idx = 0
        while True:
            with contextlib.redirect_stderr(open(os.devnull, 'w')):
                gid = codes_grib_new_from_file(fin)
            if gid is None:
                break
            try:
                # Values in GRIB-defined scanning order
                vals = np.array(codes_get_values(gid), dtype=np.float64)

                # Determine missing handling
                try:
                    mv = codes_get(gid, 'missingValue')
                except Exception:
                    mv = None

                # Best-effort bitmap (not always available)
                bitmap_mask = None
                try:
                    if codes_is_defined(gid, 'bitmapPresent') and codes_get(gid, 'bitmapPresent') == 1:
                        try:
                            bitmap = codes_get_array(gid, 'bitmap')
                            bitmap_mask = np.array(bitmap, dtype=bool)
                        except Exception:
                            bitmap_mask = None
                except Exception:
                    bitmap_mask = None

                tokens = []
                for j, v in enumerate(vals):
                    is_miss = False
                    if mv is not None and isinstance(mv, float) and not np.isnan(mv):
                        if v == mv:
                            is_miss = True
                    if not is_miss and np.isnan(v):
                        is_miss = True
                    if not is_miss and bitmap_mask is not None and j < bitmap_mask.size and not bitmap_mask[j]:
                        is_miss = True
                    tokens.append('--' if is_miss else format(float(v), '.17g'))

                xml_path = outdir / f"{prefix}_msg_{idx}.xml"
                with open(xml_path, 'w', encoding='utf-8') as xf:
                    xf.write('<variable index="%d">\n' % idx)
                    xf.write('  <values>')
                    # Avoid ultra-long single lines for big grids
                    chunk = 10000
                    for start in range(0, len(tokens), chunk):
                        if start > 0:
                            xf.write('\n')
                        xf.write(' '.join(tokens[start:start+chunk]))
                    xf.write('</values>\n')
                    xf.write('</variable>\n')
                written += 1
            finally:
                codes_release(gid)
            idx += 1
    return written


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Dump GRIB messages to XML values (full precision, GRIB order).")
    p.add_argument('--in', dest='in_grib', type=Path, default=DATA_DIR / 'small_subset_500mb.grb2', help='Input GRIB file')
    p.add_argument('--outdir', dest='outdir', type=Path, default=OUTPUT_DIR, help='Directory to write XML files')
    p.add_argument('--prefix', dest='prefix', type=str, default=None, help='Filename prefix for XML (default: input basename without extension)')
    args = p.parse_args()

    prefix = args.prefix or args.in_grib.stem
    count = dump_grib_to_xml(args.in_grib, args.outdir, prefix)
    print(f"XML dump summary: wrote {count} messages to {args.outdir}")