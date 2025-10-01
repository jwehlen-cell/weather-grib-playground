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
OUTPUT_XML = PROJECT_ROOT / 'output_xml'


def dump_grib_to_xml(in_grib: Path, outdir: Path, prefix: str) -> int:
    """Dump all GRIB messages to rich XML (values + metadata).

    XML schema (simplified):
      <gribMessage version="1" index="i">
        <ident>...</ident>
        <geometry>...</geometry>
        <representation>...</representation>
        <data>
          <bitmap>RLE</bitmap>
          <values>v1 v2 ...</values>
        </data>
      </gribMessage>

    Returns number of messages written.
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
                # Values
                vals = np.array(codes_get_values(gid), dtype=np.float64)

                # Missing value and bitmap
                try:
                    mv = codes_get(gid, 'missingValue')
                except Exception:
                    mv = None

                bitmap_mask = None
                try:
                    if codes_get(gid, 'bitmapPresent') == 1:
                        try:
                            bitmap = codes_get_array(gid, 'bitmap')
                            bitmap_mask = np.array(bitmap, dtype=bool)
                        except Exception:
                            bitmap_mask = None
                except Exception:
                    bitmap_mask = None

                # Tokenize values, preserving missing as '--'
                tokens = []
                for j, v in enumerate(vals):
                    is_miss = False
                    if mv is not None and isinstance(mv, float) and not np.isnan(mv) and v == mv:
                        is_miss = True
                    if not is_miss and np.isnan(v):
                        is_miss = True
                    if not is_miss and bitmap_mask is not None and j < bitmap_mask.size and not bitmap_mask[j]:
                        is_miss = True
                    tokens.append('--' if is_miss else format(float(v), '.17g'))

                # --- gather metadata keys (best-effort) ---
                def g(key, default=None):
                    try:
                        return codes_get(gid, key)
                    except Exception:
                        return default

                centre = g('centre')
                sub_c  = g('subCentre')
                disc   = g('discipline')
                pcat   = g('parameterCategory')
                pnum   = g('parameterNumber')
                sname  = g('shortName')
                tol    = g('typeOfLevel')
                level  = g('level')
                date   = g('date')
                time_  = g('time')
                step_t = g('stepType')
                step_r = g('stepRange')

                grid   = g('gridType')
                Ni     = g('Ni')
                Nj     = g('Nj')
                la1    = g('latitudeOfFirstGridPointInDegrees')
                lo1    = g('longitudeOfFirstGridPointInDegrees')
                di     = g('iDirectionIncrementInDegrees')
                dj     = g('jDirectionIncrementInDegrees')
                scan   = g('scanningMode')

                ptype  = g('packingType')
                drt    = g('dataRepresentationTemplateNumber')
                bpv    = g('bitsPerValue')
                bsf    = g('binaryScaleFactor')
                dsf    = g('decimalScaleFactor')
                refv   = g('referenceValue')
                mval   = g('missingValue')
                bmppr  = g('bitmapPresent')

                # Optional secondary missing value
                try:
                    mval2 = codes_get(gid, 'secondaryMissingValue')
                except Exception:
                    mval2 = None

                # Hex encodings (big-endian IEEE-754 float32) for exact restoration
                rv_hex = None
                if refv is not None:
                    try:
                        rv_hex = np.asarray(np.float32(refv), dtype='>f4').tobytes().hex()
                    except Exception:
                        rv_hex = None
                mv_hex = None
                if mval is not None:
                    try:
                        mv_hex = np.asarray(np.float32(mval), dtype='>f4').tobytes().hex()
                    except Exception:
                        mv_hex = None
                mv2_hex = None
                if mval2 is not None:
                    try:
                        mv2_hex = np.asarray(np.float32(mval2), dtype='>f4').tobytes().hex()
                    except Exception:
                        mv2_hex = None

                # optional bitmap RLE (P=present, M=missing)
                rle_str = None
                if bitmap_mask is not None:
                    runs = []
                    last = None
                    count = 0
                    for flag in bitmap_mask.tolist():
                        cur = 'P' if flag else 'M'
                        if last is None:
                            last = cur; count = 1
                        elif cur == last:
                            count += 1
                        else:
                            runs.append(f"{last}{count}")
                            last = cur; count = 1
                    if last is not None:
                        runs.append(f"{last}{count}")
                    rle_str = ' '.join(runs)

                # Write XML
                xml_path = outdir / f"{prefix}_msg_{idx}.xml"
                with open(xml_path, 'w', encoding='utf-8') as xf:
                    xf.write('<gribMessage version="1" index="%d">\n' % idx)
                    xf.write('  <ident>\n')
                    if centre is not None:  xf.write(f'    <centre>{centre}</centre>\n')
                    if sub_c is not None:   xf.write(f'    <subCentre>{sub_c}</subCentre>\n')
                    if disc is not None:    xf.write(f'    <discipline>{disc}</discipline>\n')
                    if pcat is not None:    xf.write(f'    <parameterCategory>{pcat}</parameterCategory>\n')
                    if pnum is not None:    xf.write(f'    <parameterNumber>{pnum}</parameterNumber>\n')
                    if sname is not None:   xf.write(f'    <shortName>{sname}</shortName>\n')
                    if tol is not None:     xf.write(f'    <typeOfLevel>{tol}</typeOfLevel>\n')
                    if level is not None:   xf.write(f'    <level>{level}</level>\n')
                    if date is not None:
                        hhmm = f"{time_:04d}" if isinstance(time_, int) else ''
                        xf.write(f'    <date ymd="{date}" hhmm="{hhmm}"/>\n')
                    if step_t is not None or step_r is not None:
                        xf.write(f'    <step type="{step_t if step_t is not None else ""}">\n')
                        if step_r is not None:
                            xf.write(f'      <range>{step_r}</range>\n')
                        xf.write('    </step>\n')
                    xf.write('  </ident>\n')

                    xf.write('  <geometry>\n')
                    xf.write(f'    <gridType>{grid}</gridType>\n')
                    for tag, val in (('Ni',Ni),('Nj',Nj),('latitudeOfFirstGridPointInDegrees',la1),
                                     ('longitudeOfFirstGridPointInDegrees',lo1),
                                     ('iDirectionIncrementInDegrees',di),('jDirectionIncrementInDegrees',dj),
                                     ('scanningMode',scan)):
                        if val is not None:
                            xf.write(f'    <{tag}>{val}</{tag}>\n')
                    xf.write('  </geometry>\n')

                    xf.write('  <representation>\n')
                    if ptype is not None:
                        xf.write(f'    <packingType>{ptype}</packingType>\n')
                    if drt is not None:
                        xf.write(f'    <dataRepresentationTemplateNumber>{drt}</dataRepresentationTemplateNumber>\n')
                    if bpv is not None:
                        xf.write(f'    <bitsPerValue>{bpv}</bitsPerValue>\n')
                    if bsf is not None:
                        xf.write(f'    <binaryScaleFactor>{bsf}</binaryScaleFactor>\n')
                    if dsf is not None:
                        xf.write(f'    <decimalScaleFactor>{dsf}</decimalScaleFactor>\n')
                    if refv is not None:
                        xf.write(f'    <referenceValue>{refv}</referenceValue>\n')
                    if rv_hex is not None:
                        xf.write(f'    <referenceValueHex>{rv_hex}</referenceValueHex>\n')
                    if mval is not None:
                        xf.write(f'    <missingValue>{mval}</missingValue>\n')
                    if mv_hex is not None:
                        xf.write(f'    <missingValueHex>{mv_hex}</missingValueHex>\n')
                    if mval2 is not None:
                        xf.write(f'    <secondaryMissingValue>{mval2}</secondaryMissingValue>\n')
                    if mv2_hex is not None:
                        xf.write(f'    <secondaryMissingValueHex>{mv2_hex}</secondaryMissingValueHex>\n')
                    if bmppr is not None:
                        xf.write(f'    <bitmapPresent>{bmppr}</bitmapPresent>\n')

                    # Dump all keys/arrays from the dataRepresentation namespace for exact reconstruction
                    try:
                        it = codes_keys_iterator_new(gid, 'dataRepresentation')
                        if it is not None:
                            xf.write('    <dataRepresentationKeys>\n')
                            while codes_keys_iterator_next(it):
                                kname = codes_keys_iterator_get_name(it)
                                # Some keys can be extremely large or derived; we skip the actual data values
                                if kname in ('values',):
                                    continue
                                try:
                                    size = None
                                    try:
                                        size = codes_get_size(gid, kname)
                                    except Exception:
                                        size = None
                                    if size and size > 1:
                                        arr = codes_get_array(gid, kname)
                                        # Join as comma-separated; keep integers as-is, floats with repr
                                        if arr is None:
                                            pass
                                        else:
                                            vals = []
                                            for a in arr:
                                                if isinstance(a, float):
                                                    vals.append(repr(float(a)))
                                                else:
                                                    vals.append(str(int(a)))
                                            xf.write(f'      <array name="{kname}">{",".join(vals)}</array>\n')
                                    else:
                                        val = codes_get(gid, kname)
                                        if isinstance(val, float):
                                            xf.write(f'      <key name="{kname}">{repr(float(val))}</key>\n')
                                        else:
                                            xf.write(f'      <key name="{kname}">{val}</key>\n')
                                except Exception:
                                    # Ignore keys we cannot read
                                    pass
                            codes_keys_iterator_delete(it)
                            xf.write('    </dataRepresentationKeys>\n')
                    except Exception:
                        pass

                    xf.write('  </representation>\n')

                    xf.write('  <data>\n')
                    if rle_str:
                        xf.write(f'    <bitmap>{rle_str}</bitmap>\n')
                    xf.write('    <values>')
                    # Avoid ultra-long single lines for big grids
                    chunk = 10000
                    for start in range(0, len(tokens), chunk):
                        if start > 0:
                            xf.write('\n      ')
                        xf.write(' '.join(tokens[start:start+chunk]))
                    xf.write('</values>\n')
                    xf.write('  </data>\n')
                    xf.write('</gribMessage>\n')


                written += 1
            finally:
                codes_release(gid)
            idx += 1
    return written


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Dump GRIB messages to rich XML values and metadata.")
    p.add_argument('--in', dest='in_grib', type=Path, nargs='+', required=False,
                   help='Input GRIB file(s). You can pass multiple files.')
    p.add_argument('--outdir', dest='outdir', type=Path, default=OUTPUT_XML,
                   help='Directory to write XML files (all files share this directory).')
    p.add_argument('--prefix', dest='prefix', type=str, default=None,
                   help='Optional filename prefix override. If omitted, each file uses its own basename.')
    args = p.parse_args()

    inputs = args.in_grib or [DATA_DIR / 'small_subset_500mb.grb2']

    total_written = 0
    for in_path in inputs:
        this_prefix = args.prefix or in_path.stem
        count = dump_grib_to_xml(in_path, args.outdir, this_prefix)
        print(f"XML dump summary [{in_path.name}]: wrote {count} messages to {args.outdir} (prefix='{this_prefix}')")
        total_written += count
    print(f"DONE. Files processed: {len(inputs)} | Total messages written: {total_written}")