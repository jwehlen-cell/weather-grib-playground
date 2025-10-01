import xarray as xr

def inspect_grib(filepath: str):
    ds = xr.open_dataset(filepath, engine="cfgrib")
    print("Variables in file:", list(ds.data_vars))
    print("Coordinates:", list(ds.coords))
    print("\nPreview:")
    print(ds)

if __name__ == "__main__":
    inspect_grib("data/QPF06hr_00z.grb")
    