import xarray as xr
import tempfile

SMALL = "data/small_subset_500mb.grb2"

def main():
    # use a writable temp dir for cfgrib index files
    cache_dir = tempfile.gettempdir()
    ds = xr.open_dataset(
        SMALL,
        engine="cfgrib",
        backend_kwargs={"indexpath": f"{cache_dir}/cfgrib_index_small"}
    )
    print("Data variables:", list(ds.data_vars))
    print("Coordinates:", list(ds.coords))
    print("\nDataset summary:\n", ds)
    ds.close()

if __name__ == "__main__":
    main()