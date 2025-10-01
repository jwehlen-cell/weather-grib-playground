#!/usr/bin/env python3
import os
os.environ.setdefault('ECCODES_LOG_STREAM', os.devnull)

import pygrib
import numpy as np
from pathlib import Path


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'

def compare_grib_files(original_path, reconstructed_path):
    orig_grbs = pygrib.open(str(original_path))
    recon_grbs = pygrib.open(str(reconstructed_path))
    
    orig_messages = list(orig_grbs)
    recon_messages = list(recon_grbs)
    
    orig_grbs.close()
    recon_grbs.close()
    
    if len(orig_messages) != len(recon_messages):
        print(f"Message count mismatch: Original {len(orig_messages)}, Reconstructed {len(recon_messages)}")
        return False
    
    all_match = True
    for index, (orig_msg, recon_msg) in enumerate(zip(orig_messages, recon_messages)):
        # Compare metadata
        if orig_msg.name != recon_msg.name:
            print(f"Message {index}: Name mismatch - Orig: {orig_msg.name}, Recon: {recon_msg.name}")
            all_match = False
        if orig_msg.validDate != recon_msg.validDate:
            print(f"Message {index}: validDate mismatch - Orig: {orig_msg.validDate}, Recon: {recon_msg.validDate}")
            all_match = False
        if orig_msg.typeOfLevel != recon_msg.typeOfLevel:
            print(f"Message {index}: typeOfLevel mismatch - Orig: {orig_msg.typeOfLevel}, Recon: {recon_msg.typeOfLevel}")
            all_match = False
        
        # Compare data values (with tolerance for floating point and handling NaNs/missing)
        orig_values = orig_msg.values
        recon_values = recon_msg.values
        
        if orig_values.shape != recon_values.shape:
            print(f"Message {index}: Values shape mismatch - Orig: {orig_values.shape}, Recon: {recon_values.shape}")
            all_match = False
            continue
        
        # Mask missing values (NaNs in pygrib)
        orig_mask = np.isnan(orig_values)
        recon_mask = np.isnan(recon_values)
        if not np.array_equal(orig_mask, recon_mask):
            print(f"Message {index}: Missing value mask mismatch")
            all_match = False
        
        # Compare non-missing values with relative tolerance
        if not np.allclose(orig_values[~orig_mask], recon_values[~recon_mask], rtol=1e-5, atol=1e-8):
            print(f"Message {index}: Data values do not match closely")
            all_match = False
    
    if all_match:
        print("All messages match: Reconstruction is lossless in data and key metadata.")
    else:
        print("Some mismatches found: Reconstruction may have losses.")
    
    return all_match

# Main execution
if __name__ == "__main__":
    # Compare large files
    #compare_grib_files(
    #    DATA_DIR / 'large_full_1deg.grb2',
    #    DATA_DIR / 'reconstructed_large.grb2'
    #)
    
    # Compare small files
    compare_grib_files(
        DATA_DIR / 'small_subset_500mb.grb2',
        DATA_DIR / 'reconstructed_small.grb2'
    )