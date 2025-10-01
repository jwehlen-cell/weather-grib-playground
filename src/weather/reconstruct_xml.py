#!/usr/bin/env python3
import argparse
import contextlib
import os
from pathlib import Path
import xml.etree.ElementTree as ET

# Silence ecCodes logging (must be set before importing eccodes)
os.environ.setdefault('ECCODES_LOG_STREAM', os.devnull)

import numpy as np
from eccodes import *  # noqa: F401,F403

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output_xml'


ess_errnull = open(os.devnull, 'w')


def _read_values_from_xml(xml_path: Path) -> np.ndarray:
    tree = ET.parse(str(xml_path))
    root = tree.getroot()
    node = root.find('variable/values') or root.find('.//values')
    if node is None:
        raise ValueError(f"Could not find <values> node in {xml_path}")
    text = (node.text or '').strip()
    if text:
        raw_tokens = [t for part in text.splitlines() for t in part.replace(',', ' ').split()]
    else:
        raw_tokens = [(child.text or '').strip() for child in list(node) if child.tag.lower() == 'value']
    if not raw_tokens:
        raise ValueError(f"No numeric tokens found in <values> for {xml_path}")
    def _to_float(tok: str) -> float:
        t = tok.strip()
        if t in {'--', 'NaN', 'nan', ''}:
            return np.nan
        return float(t)
    return np.fromiter((_to_float(t) for t in raw_tokens), dtype=np.float64)


def decode_xml_to_grib(original_grb_path: Path, reconstructed_grb_path: Path, xml_prefix: str, packing_mode: str = 'original') -> None:
    reconstructed = 0
    with open(original_grb_path, 'rb') as fin, open(reconstructed_grb_path, 'wb') as fout:
        msg_index = 0
        while True:
            with contextlib.redirect_stderr(ess_errnull):
                gid = codes_grib_new_from_file(fin)
            if gid is None:
                break
            try:
                with contextlib.redirect_stderr(ess_errnull):
                    clone_id = codes_clone(gid)

                # Optional: switch to IEEE packing up front (exact decoded values)
                force_ieee = (packing_mode in ('ieee32', 'ieee64'))
                if force_ieee:
                    with contextlib.redirect_stderr(ess_errnull):
                        try:
                            codes_set(clone_id, 'packingType', 'grid_ieee')
                        except Exception:
                            pass
                        try:
                            # precision: 1 -> 32-bit, 2 -> 64-bit
                            codes_set(clone_id, 'precision', 1 if packing_mode == 'ieee32' else 2)
                        except Exception:
                            pass

                # Load XML for this message; require presence for lossless round-trip
                xml_path = OUTPUT_DIR / f"{xml_prefix}_msg_{msg_index}.xml"
                if not xml_path.exists():
                    raise FileNotFoundError(f"Missing XML for message {msg_index}: {xml_path}")

                values = _read_values_from_xml(xml_path)
                with contextlib.redirect_stderr(ess_errnull):
                    expected = codes_get(gid, 'numberOfDataPoints')
                if values.size != expected:
                    raise ValueError(f"Value count mismatch for msg {msg_index}: got {values.size}, expected {expected}.")

                # Keep or map missing values depending on packing mode
                try:
                    msg_missing = codes_get(gid, 'missingValue')
                except Exception:
                    msg_missing = float('nan')

                if values.dtype != np.float64:
                    values = values.astype(np.float64, copy=False)

                if np.isnan(values).any():
                    if packing_mode == 'original':
                        if not (isinstance(msg_missing, float) and np.isnan(msg_missing)):
                            values = np.where(np.isnan(values), msg_missing, values)
                            try:
                                if codes_is_defined(clone_id, 'bitmapPresent'):
                                    codes_set(clone_id, 'bitmapPresent', 1)
                            except Exception:
                                pass
                    else:
                        # In IEEE modes, keep NaNs; no bitmap mapping required
                        pass

                # In original mode, restore original representation knobs BEFORE setting values
                if packing_mode == 'original':
                    with contextlib.redirect_stderr(ess_errnull):
                        # packing type and template number
                        try:
                            orig_ptype = codes_get(gid, 'packingType')
                            codes_set(clone_id, 'packingType', orig_ptype)
                        except Exception:
                            pass
                        try:
                            drt = codes_get(gid, 'dataRepresentationTemplateNumber')
                            codes_set(clone_id, 'dataRepresentationTemplateNumber', drt)
                        except Exception:
                            pass
                        # Section 5 parameters
                        for k in ('bitsPerValue', 'binaryScaleFactor', 'decimalScaleFactor', 'referenceValue'):
                            try:
                                v = codes_get(gid, k)
                                codes_set(clone_id, k, v)
                            except Exception:
                                pass
                        try:
                            codes_set(clone_id, 'useOriginalPacking', 1)
                        except Exception:
                            pass

                # Set values and write out
                codes_set_values(clone_id, values)
                with contextlib.redirect_stderr(ess_errnull):
                    codes_write(clone_id, fout)
                reconstructed += 1

                # Optional read-back to trip errors early (non-fatal)
                try:
                    with contextlib.redirect_stderr(ess_errnull):
                        _ = codes_get(clone_id, 'totalLength')
                except Exception:
                    pass

            finally:
                try:
                    codes_release(clone_id)
                except Exception:
                    pass
                codes_release(gid)
            msg_index += 1

    print(f"Reconstruction summary: {reconstructed} messages from XML.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Reconstruct GRIB2 from XML value dumps (supports multiple files).')
    parser.add_argument('--in', dest='in_grib', type=Path, nargs='+', required=False,
                        help='Input GRIB file(s) to clone structure from. You can pass multiple files.')
    parser.add_argument('--out', dest='out_grib', type=Path, nargs='*', default=None,
                        help='Output GRIB path(s). If omitted, defaults to data/reconstructed_<in_stem>.grb2 for each input.')
    parser.add_argument('--prefix', dest='xml_prefix', nargs='*', default=None,
                        help='XML filename prefix(es). If omitted, uses each input stem.')
    parser.add_argument('--packing', dest='packing', choices=['original', 'ieee32', 'ieee64'], default='original',
                        help='How to pack reconstructed fields: original (default), ieee32, or ieee64.')
    args = parser.parse_args()

    inputs = args.in_grib or [DATA_DIR / 'small_subset_500mb.grb2']

    # Normalize outputs and prefixes to per-input lists
    out_paths = []
    prefixes = []
    if args.out_grib and len(args.out_grib) not in (0, len(inputs)):
        raise SystemExit('--out must be provided once per input or omitted entirely')
    if args.xml_prefix and len(args.xml_prefix) not in (0, len(inputs)):
        raise SystemExit('--prefix must be provided once per input or omitted entirely')

    for i, in_path in enumerate(inputs):
        out_path = args.out_grib[i] if args.out_grib else DATA_DIR / f'reconstructed_{in_path.stem}.grb2'
        pref = args.xml_prefix[i] if args.xml_prefix else in_path.stem
        out_paths.append(out_path)
        prefixes.append(pref)

    packing_mode = args.packing

    for in_path, out_path, pref in zip(inputs, out_paths, prefixes):
        decode_xml_to_grib(in_path, out_path, pref, packing_mode)