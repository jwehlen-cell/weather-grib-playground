#!/usr/bin/env python3
import os
os.environ.setdefault('ECCODES_LOG_STREAM', os.devnull)

import sys
from pathlib import Path
import numpy as np
from eccodes import *

def read_xml_values(xml_path: Path):
    import xml.etree.ElementTree as ET
    root = ET.parse(str(xml_path)).getroot()
    node = root.find('variable/values') or root.find('.//values')
    if node is None:
        raise SystemExit(f"No <values> in {xml_path}")
    text = (node.text or '').strip()
    if text:
        toks = [t for line in text.splitlines() for t in line.replace(',', ' ').split()]
    else:
        toks = [(ch.text or '').strip() for ch in list(node) if ch.tag.lower() == 'value']
    def f(tok):
        t = tok.strip()
        if t in {'--', 'NaN', 'nan', ''}: return np.nan
        return float(t)
    return np.fromiter((f(t) for t in toks), dtype=np.float64)

if len(sys.argv) not in (3,4):
    print(f"Usage: {Path(sys.argv[0]).name} <original.grb2> <xml_prefix> [msg_index]")
    sys.exit(1)

grb_path = Path(sys.argv[1])
prefix   = sys.argv[2]
msg_idx  = int(sys.argv[3]) if len(sys.argv)==4 else 0
xml_path = Path('output_xml') / f"{prefix}_msg_{msg_idx}.xml"

fo = open(grb_path, 'rb')
for i in range(msg_idx+1):
    gid = codes_grib_new_from_file(fo)
    if gid is None:
        print(f"Original has fewer than {msg_idx+1} messages.")
        sys.exit(2)

vals = np.array(codes_get_values(gid), dtype=np.float64)
codes_release(gid); fo.close()

xvals = read_xml_values(xml_path)
if vals.size != xvals.size:
    print(f"Count mismatch: GRIB {vals.size} vs XML {xvals.size}")
    sys.exit(3)

diff = np.abs(vals - xvals)
maxd = np.nanmax(diff)
print(f"Msg {msg_idx}: max abs diff (original vs XML) = {maxd}")