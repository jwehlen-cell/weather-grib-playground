#!/usr/bin/env python3
import pygrib
import xml.etree.ElementTree as ET
import xmlschema
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output_xml'
XSD_FILE = PROJECT_ROOT / 'grib.xsd'

OUTPUT_DIR.mkdir(exist_ok=True)

def process_message(msg, index, grb_filename):
    root = ET.Element('grib')
    var = ET.SubElement(root, 'variable', name=msg.name, date=str(msg.validDate), level=msg.typeOfLevel)
    ET.SubElement(var, 'values').text = ' '.join(map(str, msg.values.flatten()))
    ET.SubElement(var, 'lat').text = ' '.join(map(str, msg.latitudes.flatten()))
    ET.SubElement(var, 'lon').text = ' '.join(map(str, msg.longitudes.flatten()))
    
    xml_filename = f"{os.path.splitext(grb_filename)[0]}_msg_{index}.xml"
    xml_path = OUTPUT_DIR / xml_filename
    ET.ElementTree(root).write(str(xml_path))
    
    schema = xmlschema.XMLSchema(str(XSD_FILE))
    schema.validate(str(xml_path))

def convert_grib_to_xml(grb_path):
    grbs = pygrib.open(str(grb_path))
    messages = list(grbs)
    grbs.close()
    
    grb_filename = grb_path.name
    
    with ThreadPoolExecutor() as executor:
        executor.map(lambda x: process_message(x[1], x[0], grb_filename), enumerate(messages))

# Main execution
if __name__ == "__main__":
    grib_files = [
        DATA_DIR / 'small_subset_500mb.grb2'
    ]
    
    for grib_file in grib_files:
        if grib_file.exists():
            convert_grib_to_xml(grib_file)