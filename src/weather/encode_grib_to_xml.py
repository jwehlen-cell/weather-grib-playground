#!/usr/bin/env python3
import pygrib
import xml.etree.ElementTree as ET
import xmlschema
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path  # Add this if missing

# Project paths (based on your structure)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # Resolves symlinks
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output_xml'
XSD_FILE = PROJECT_ROOT / 'grib.xsd'

# Print paths for debugging
print(f"Project root: {PROJECT_ROOT}")
print(f"Data dir: {DATA_DIR}")
print(f"Output dir: {OUTPUT_DIR}")
print(f"XSD file: {XSD_FILE}")

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(exist_ok=True)

# Function to process a single GRIB message into an XML file
def process_message(msg, index, grb_filename):
    root = ET.Element('grib')
    var = ET.SubElement(root, 'variable', name=msg.name, date=str(msg.validDate), level=msg.typeOfLevel)
    ET.SubElement(var, 'values').text = ' '.join(map(str, msg.values.flatten()))
    ET.SubElement(var, 'lat').text = ' '.join(map(str, msg.latitudes.flatten()))
    ET.SubElement(var, 'lon').text = ' '.join(map(str, msg.longitudes.flatten()))
    
    # Name XML based on original GRIB and message index
    xml_filename = f"{os.path.splitext(grb_filename)[0]}_msg_{index}.xml"
    xml_path = OUTPUT_DIR / xml_filename
    ET.ElementTree(root).write(str(xml_path))
    
    # Validate against XSD
    schema = xmlschema.XMLSchema(str(XSD_FILE))
    schema.validate(str(xml_path))
    
    print(f"Generated and validated: {xml_path}")

# Function to convert a single GRIB file (sequential for debugging)
def convert_grib_to_xml(grb_path):
    grbs = pygrib.open(str(grb_path))
    messages = list(grbs)
    grbs.close()
    
    grb_filename = grb_path.name
    print(f"Number of messages in {grb_path}: {len(messages)}")
    
    # Process sequentially with error handling
    for index, msg in enumerate(messages):
        try:
            print(f"Processing message {index} for {grb_filename}...")
            root = ET.Element('grib')
            var = ET.SubElement(root, 'variable', name=msg.name, date=str(msg.validDate), level=msg.typeOfLevel)
            values_text = ' '.join(map(str, msg.values.flatten()))
            lat_text = ' '.join(map(str, msg.latitudes.flatten()))
            lon_text = ' '.join(map(str, msg.longitudes.flatten()))
            ET.SubElement(var, 'values').text = values_text
            ET.SubElement(var, 'lat').text = lat_text
            ET.SubElement(var, 'lon').text = lon_text
            
            xml_filename = f"{os.path.splitext(grb_filename)[0]}_msg_{index}.xml"
            xml_path = OUTPUT_DIR / xml_filename
            print(f"Attempting to write: {xml_path}")
            
            ET.ElementTree(root).write(str(xml_path))  # Use str() for compatibility
            
            schema = xmlschema.XMLSchema(str(XSD_FILE))
            schema.validate(str(xml_path))
            print(f"Successfully generated and validated: {xml_path}")
        except Exception as e:
            print(f"Error processing message {index} for {grb_filename}: {str(e)}")
    
    print(f"Conversion complete for {grb_path}. XML files in {OUTPUT_DIR}")

# Main execution
if __name__ == "__main__":
    # List your GRIB files from data/ (using Path objects)
    grib_files = [
        DATA_DIR / 'large_full_1deg.grb2',
        DATA_DIR / 'small_subset_500mb.grb2'
    ]
    
    for grib_file in grib_files:
        if grib_file.exists():
            convert_grib_to_xml(grib_file)
        else:
            print(f"File not found: {grib_file}")