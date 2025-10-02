#!/usr/bin/env python3
# Reconstruct GRIB files from XML/value dumps, or via byte-identical concatenation
import argparse
import contextlib
import os
from pathlib import Path
import xml.etree.ElementTree as ET

# Silence ecCodes logging/debug BEFORE importing eccodes
os.environ.setdefault('ECCODES_LOG_STREAM', os.devnull)
os.environ.setdefault('ECCODES_DEBUG', '0')

import numpy as np
from eccodes import *  # noqa: F401,F403

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_XML = PROJECT_ROOT / 'output_xml'

# shared stderr sink for noisy library messages
_ESS_NULL = open(os.devnull, 'w')


def _read_from_xml(xml_path: Path):
    """Read values and best-effort metadata from an XML produced by convert_grb.py.
    Returns: (values: np.ndarray float64, meta: dict)
    Compatible with both <gribMessage> and legacy <values> layout.
    """
    tree = ET.parse(str(xml_path))
    root = tree.getroot()

    # Values node (avoid truthiness deprecation on Element)
    vnode = root.find('data/values')
    if vnode is None:
        vnode = root.find('.//values')
    if vnode is None:
        raise ValueError(f"Could not find <values> in {xml_path}")

    text = (vnode.text or '').strip()
    if text:
        toks = [t for part in text.splitlines() for t in part.replace(',', ' ').split()]
    else:
        toks = [(child.text or '').strip() for child in list(vnode) if (getattr(child, 'tag', '').lower() == 'value')]

    def _to_float(tok: str) -> float:
        t = tok.strip()
        if t in {'--', 'NaN', 'nan', ''}:
            return np.nan
        return float(t)

    values = np.fromiter((_to_float(t) for t in toks), dtype=np.float64)

    # Meta (best-effort)
    def gx(path, cast=str, default=None):
        node = root.find(path)
        if node is None:
            return default
        try:
            # some numeric nodes may be empty
            return cast(node.text) if node.text is not None else default
        except Exception:
            return default

    meta = {
        'packingType': gx('representation/packingType'),
        'dataRepresentationTemplateNumber': gx('representation/dataRepresentationTemplateNumber', int),
        'bitsPerValue': gx('representation/bitsPerValue', int),
        'binaryScaleFactor': gx('representation/binaryScaleFactor', int),
        'decimalScaleFactor': gx('representation/decimalScaleFactor', int),
        'referenceValue': gx('representation/referenceValue', float),
        'missingValue': gx('representation/missingValue', float),
        'bitmapPresent': gx('representation/bitmapPresent', int),
        'gridType': gx('geometry/gridType'),
        'Ni': gx('geometry/Ni', int),
        'Nj': gx('geometry/Nj', int),
        'referenceValueHex': gx('representation/referenceValueHex', str),
        'missingValueHex': gx('representation/missingValueHex', str),
        'secondaryMissingValue': gx('representation/secondaryMissingValue', float),
        'secondaryMissingValueHex': gx('representation/secondaryMissingValueHex', str),
    }

    # Capture full Section 5 namespace dump (<dataRepresentationKeys>)
    repr_keys = {}
    repr_arrays = {}
    drk = root.find('representation/dataRepresentationKeys')
    if drk is not None:
        # Scalar keys
        for kn in drk.findall('key'):
            name = kn.get('name')
            if not name:
                continue
            txt = (kn.text or '').strip()
            if txt == '':
                continue
            # Prefer int, then float, else keep string
            try:
                val = int(txt)
            except Exception:
                try:
                    val = float(txt)
                except Exception:
                    val = txt
            repr_keys[name] = val
        # Array keys
        for an in drk.findall('array'):
            name = an.get('name')
            if not name:
                continue
            txt = (an.text or '').strip()
            if txt == '':
                continue
            parts = [p.strip() for p in txt.split(',') if p.strip() != '']
            arr = []
            for p in parts:
                try:
                    arr.append(int(p))
                except Exception:
                    try:
                        arr.append(float(p))
                    except Exception:
                        # skip
                        pass
            repr_arrays[name] = arr
    meta['repr_keys'] = repr_keys
    meta['repr_arrays'] = repr_arrays

    # Optional codedValues (comma/newline separated integers)
    coded_vals = None
    cvnode = root.find('representation/codedValues')
    if cvnode is not None and (cvnode.text or '').strip():
        txt = cvnode.text.strip()
        parts = [p.strip() for p in txt.replace('\n', ',').split(',') if p.strip()]
        try:
            coded_vals = [int(p) for p in parts]
        except Exception:
            coded_vals = None
    meta['codedValues'] = coded_vals

    # Optional bitmap RLE -> boolean mask
    bnode = root.find('data/bitmap')
    if bnode is not None and bnode.text:
        runs = bnode.text.strip().split()
        mask = []
        for run in runs:
            if not run:
                continue
            tag = run[0]
            n = int(run[1:]) if len(run) > 1 else 0
            flag = (tag == 'P')
            mask.extend([flag] * n)
        meta['bitmap'] = np.array(mask, dtype=bool)

    return values, meta


def decode_xml_to_grib(original_grb_path: Path, reconstructed_grb_path: Path, xml_prefix: str, packing_mode: str = 'original') -> None:
    reconstructed = 0
    reconstructed_grb_path.parent.mkdir(parents=True, exist_ok=True)

    with open(original_grb_path, 'rb') as fin, open(reconstructed_grb_path, 'wb') as fout:
        msg_index = 0
        while True:
            with contextlib.redirect_stderr(_ESS_NULL):
                gid = codes_grib_new_from_file(fin)
            if gid is None:
                break
            try:
                with contextlib.redirect_stderr(_ESS_NULL):
                    clone_id = codes_clone(gid)

                # Optional IEEE packing for exact decoded values
                force_ieee = (packing_mode in ('ieee32', 'ieee64'))
                if force_ieee:
                    with contextlib.redirect_stderr(_ESS_NULL):
                        try:
                            codes_set(clone_id, 'packingType', 'grid_ieee')
                        except Exception:
                            pass
                        try:
                            # precision: 1 -> 32-bit, 2 -> 64-bit
                            codes_set(clone_id, 'precision', 1 if packing_mode == 'ieee32' else 2)
                        except Exception:
                            pass

                # Load XML for this message
                xml_path = OUTPUT_XML / f"{xml_prefix}_msg_{msg_index}.xml"
                if not xml_path.exists():
                    raise FileNotFoundError(f"Missing XML for message {msg_index}: {xml_path}")

                values, meta = _read_from_xml(xml_path)
                with contextlib.redirect_stderr(_ESS_NULL):
                    expected = codes_get(gid, 'numberOfDataPoints')
                if values.size != expected:
                    raise ValueError(f"Value count mismatch for msg {msg_index}: got {values.size}, expected {expected}.")

                # Missing value handling
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
                        # IEEE modes preserve NaNs
                        pass

                # In original mode, prefer representation from XML, then fall back to source gid
                if packing_mode == 'original':
                    with contextlib.redirect_stderr(_ESS_NULL):
                        # Prefer meta from XML when present
                        try:
                            if meta.get('packingType'):
                                codes_set(clone_id, 'packingType', meta['packingType'])
                            if meta.get('dataRepresentationTemplateNumber') is not None:
                                codes_set(clone_id, 'dataRepresentationTemplateNumber', meta['dataRepresentationTemplateNumber'])
                            # Restore full Section 5 namespace keys/arrays when provided
                            rk = meta.get('repr_keys') or {}
                            ra = meta.get('repr_arrays') or {}
                            for name, val in rk.items():
                                try:
                                    codes_set(clone_id, name, val)
                                except Exception:
                                    pass
                            for name, arr in ra.items():
                                try:
                                    codes_set_array(clone_id, name, arr)
                                except Exception:
                                    pass
                            # If codedValues are provided, set them directly to avoid re-quantization
                            coded_vals = meta.get('codedValues')
                            if coded_vals:
                                try:
                                    codes_set_array(clone_id, 'codedValues', coded_vals)
                                    # When coded values are set, do not also set decoded values later
                                    values = None
                                except Exception:
                                    pass
                            # Integer knobs
                            for k in ('bitsPerValue', 'binaryScaleFactor', 'decimalScaleFactor'):
                                if meta.get(k) is not None:
                                    codes_set(clone_id, k, meta[k])
                            # referenceValue via hex preferred
                            ref_hex = meta.get('referenceValueHex')
                            if ref_hex:
                                try:
                                    rv = np.frombuffer(bytes.fromhex(ref_hex), dtype='>f4')[0]
                                    codes_set(clone_id, 'referenceValue', float(rv))
                                except Exception:
                                    if meta.get('referenceValue') is not None:
                                        codes_set(clone_id, 'referenceValue', meta['referenceValue'])
                            elif meta.get('referenceValue') is not None:
                                codes_set(clone_id, 'referenceValue', meta['referenceValue'])
                            # missingValue via hex preferred
                            mv_hex = meta.get('missingValueHex')
                            if mv_hex:
                                try:
                                    mv = np.frombuffer(bytes.fromhex(mv_hex), dtype='>f4')[0]
                                    codes_set(clone_id, 'missingValue', float(mv))
                                except Exception:
                                    if meta.get('missingValue') is not None:
                                        codes_set(clone_id, 'missingValue', meta['missingValue'])
                            elif meta.get('missingValue') is not None:
                                codes_set(clone_id, 'missingValue', meta['missingValue'])
                            # secondaryMissingValue via hex preferred
                            mv2_hex = meta.get('secondaryMissingValueHex')
                            if mv2_hex:
                                try:
                                    mv2 = np.frombuffer(bytes.fromhex(mv2_hex), dtype='>f4')[0]
                                    codes_set(clone_id, 'secondaryMissingValue', float(mv2))
                                except Exception:
                                    if meta.get('secondaryMissingValue') is not None:
                                        codes_set(clone_id, 'secondaryMissingValue', meta['secondaryMissingValue'])
                            elif meta.get('secondaryMissingValue') is not None:
                                codes_set(clone_id, 'secondaryMissingValue', meta['secondaryMissingValue'])
                        except Exception:
                            pass
                        # Fallback to original gid knobs
                        for k in (
                            'packingType',
                            'dataRepresentationTemplateNumber',
                            'bitsPerValue',
                            'binaryScaleFactor',
                            'decimalScaleFactor',
                            'referenceValue',
                        ):
                            try:
                                if not codes_is_defined(clone_id, k):
                                    v = codes_get(gid, k)
                                    codes_set(clone_id, k, v)
                            except Exception:
                                pass
                        try:
                            codes_set(clone_id, 'useOriginalPacking', 1)
                        except Exception:
                            pass

                # Write values
                with contextlib.redirect_stderr(_ESS_NULL):
                    if values is not None:
                        codes_set_values(clone_id, values)
                    codes_write(clone_id, fout)
                reconstructed += 1

                # Non-fatal check
                try:
                    with contextlib.redirect_stderr(_ESS_NULL):
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
    parser.add_argument('--in', dest='in_grb', type=Path, nargs='+', required=False,
                        help='Input GRIB file(s) to clone structure from. You can pass multiple files.')
    parser.add_argument('--out', dest='out_grib', type=Path, nargs='*', default=None,
                        help='Output GRIB path(s). If omitted, defaults to data/reconstructed_<in_stem>.grb2 for each input.')
    parser.add_argument('--prefix', dest='xml_prefix', nargs='*', default=None,
                        help='XML filename prefix(es). If omitted, uses each input stem.')
    parser.add_argument('--packing', dest='packing', choices=['original', 'ieee32', 'ieee64'], default='original',
                        help='How to pack reconstructed fields: original (default), ieee32, or ieee64.')
    args = parser.parse_args()

    inputs = args.in_grb or [DATA_DIR / 'small_subset_500mb.grb2']

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

    for in_path, out_path, pref in zip(inputs, out_paths, prefixes):
        decode_xml_to_grib(in_path, out_path, pref, args.packing)