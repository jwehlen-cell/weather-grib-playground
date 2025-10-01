import xarray as xr
import tempfile

LARGE = "data/large_full_1deg.grb2"

def open_group(path, type_of_level):
    cache_dir = tempfile.gettempdir()
    return xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "indexpath": f"{cache_dir}/cfgrib_index_large_{type_of_level}",
            "filter_by_keys": {"typeOfLevel": type_of_level},
        },
    )

def main():
    # Try pressure levels first; fall back to surface
    try:
        ds = open_group(LARGE, "isobaricInhPa")
        group = "isobaricInhPa"
    except Exception:
        ds = open_group(LARGE, "surface")
        group = "surface"

    print(f"Opened group: {group}")
    print("Dims:", dict(ds.sizes))
    print("Coords:", list(ds.coords))
    print("Vars:", list(ds.data_vars))

    # 1) Index positional dims safely (if they exist)
    if "time" in ds.dims:
        ds = ds.isel(time=0)
    if "step" in ds.dims:
        ds = ds.isel(step=0)

    # 2) Select 500 hPa by label if available
    if "isobaricInhPa" in ds.coords and 500 in ds.isobaricInhPa.values:
        ds = ds.sel(isobaricInhPa=500)

    # 3) Peek at the first variable on a tiny window
    var_name = list(ds.data_vars)[0]
    da = ds[var_name]
    slicer = {}
    if "latitude" in da.dims:
        slicer["latitude"] = slice(0, 10)
    if "longitude" in da.dims:
        slicer["longitude"] = slice(0, 10)
    da_small = da.isel(**slicer) if slicer else da

    print(f"\nPeek var={var_name}, full_shape={da.shape}, window_shape={da_small.shape}")
    print("Min/Max on small window:", float(da_small.min()), float(da_small.max()))

    ds.close()

if __name__ == "__main__":
    main()