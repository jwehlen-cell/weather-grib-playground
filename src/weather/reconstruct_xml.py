#!/usr/bin/env python3
import numpy as np
from eccodes import *
import xml.etree.ElementTree as ET
from pathlib import Path
import contextlib
import os

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output_xml'

def decode_xml_to_grib(original_grb_path, reconstructed_grb_path, xml_prefix):
    fin = open(str(original_grb_path), 'rb')
    fout = open(str(reconstructed_grb_path), 'wb')
    
    msg_index = 0
    while True:
        gid = codes_grib_new_from_file(fin)
        if gid is None:
            break
        
        clone_id = codes_clone(gid)
        
        # Set lossless IEEE packing with double precision
        with contextlib.redirect_stderr(open(os.devnull, 'w')):
            codes_set(clone_id, 'packingType', 'grid_ieee')
            codes_set(clone_id, 'precision', 2)  # 2 = double (64-bit)
        
        xml_filename = f"{xml_prefix}_msg_{msg_index}.xml"
        xml_path = OUTPUT_DIR / xml_filename
        if not xml_path.exists():
            raise FileNotFoundError(f"Missing XML: {xml_path}")
        
        tree = ET.parse(str(xml_path))
        root = tree.getroot()
        var = root.find('variable')
        values_text = var.find('values').text
        
        missing_value = codes_get(gid, 'missingValue')
        
        values_list = []
        for s in values_text.split():
            if s == '--':
                values_list.append(missing_value)
            else:
                values_list.append(float(s))
        
        values = np.array(values_list)
        
        codes_set_values(clone_id, values)
        
        with contextlib.redirect_stderr(open(os.devnull, 'w')):
            codes_write(clone_id, fout)
        
        codes_release(clone_id)
        codes_release(gid)
        
        msg_index += 1
    
    fin.close()
    fout.close()

# Main execution
if __name__ == "__main__":
    # Reconstruct small file only
    decode_xml_to_grib(
        DATA_DIR / 'small_subset_500mb.grb2',
        PROJECT_ROOT / 'reconstructed_small.grb2',
        'small_subset_500mb'
    )