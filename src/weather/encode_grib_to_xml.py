import pygrib
import xml.etree.ElementTree as ET
import xmlschema
import os
from concurrent.futures import ProcessPoolExecutor

# Define paths based on your environment
DATA_DIR = 'data'
OUTPUT_DIR = 'output_xml'  # We'll create this folder for XML files
XSD_FILE = 'grib.xsd'  # Assume you've saved the XSD here

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Function to process a single GRIB message into an XML file
def process_message(msg, index, grb_filename):
    root = ET.Element('grib')
    var = ET.SubElement(root, 'variable', name=msg.name, date=str(msg.validDate), level=msg.typeOfLevel)
    ET.SubElement(var, 'values').text = ' '.join(map(str, msg.values.flatten()))
    ET.SubElement(var, 'lat').text = ' '.join(map(str, msg.latitudes.flatten()))
    ET.SubElement(var, 'lon').text = ' '.join(map(str, msg.longitudes.flatten()))
    
    # Name XML based on original GRIB and message index
    xml_filename = f"{os.path.splitext(grb_filename)[0]}_msg_{index}.xml"
    xml_path = os.path.join(OUTPUT_DIR, xml_filename)
    ET.ElementTree(root).write(xml_path)
    
    # Validate against XSD
    schema = xmlschema.XMLSchema(XSD_FILE)
    schema.validate(xml_path)
    
    print(f"Generated and validated: {xml_path}")

# Function to convert a single GRIB file
def convert_grib_to_xml(grb_path):
    grbs = pygrib.open(grb_path)
    messages = list(grbs)  # Load all messages
    grbs.close()
    
    grb_filename = os.path.basename(grb_path)
    
    # Process messages in parallel for speed
    with ProcessPoolExecutor() as executor:
        executor.map(lambda x: process_message(x[1], x[0], grb_filename), enumerate(messages))
    
    print(f"Conversion complete for {grb_path}. XML files in {OUTPUT_DIR}")

# Main execution
if __name__ == "__main__":
    # Assuming you have the XSD saved as 'grib.xsd' in the project root
    # If not, create it with the content from earlier discussions
    
    # List your GRIB files
    grib_files = [
        os.path.join(DATA_DIR, 'large_full_1deg.grb2'),
        os.path.join(DATA_DIR, 'small_subset_500mb.grb2')
    ]
    
    for grib_file in grib_files:
        if os.path.exists(grib_file):
            convert_grib_to_xml(grib_file)
        else:
            print(f"File not found: {grib_file}")