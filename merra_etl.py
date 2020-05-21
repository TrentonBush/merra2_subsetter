import xarray as xr
import numpy as np
from pathlib import Path
from typing import Optional, Union


def binary_round(
    ds: Union[xr.Dataset, xr.DataArray, np.ndarray],
    *,
    binary_bits: Optional[int] = None,
    decimal_digits: Optional[int] = None,
) -> Union[xr.Dataset, xr.DataArray, np.ndarray]:
    """Round an array of floats to a specific number of bits. The number of bits can be specified directly, with binary_bits, or converted from decimal_digits. The decimal_digits conversion will always round with at least as much precision as the decimal_digits.
    Note: input negative bits to round to the left of the floating point.
    Motivation is to enable higher compression by removing false precision from data.
    
    Example: binary_round(np.array([1.1234567]), decimal_digits=2) -> np.ndarray(1.125)

    Parameters
    ----------
    ds : Union[xr.Dataset, xr.DataArray, np.ndarray]
        dataset
    binary_bits : Optional[int], optional
        Number of bits of precision to keep. Choose this OR decimal_digits. By default None
    decimal_digits : Optional[int], optional
        Number of decimal digits of precision to keep. Choose this OR binary_bits. By default None

    Returns
    -------
    Union[xr.Dataset, xr.DataArray, np.ndarray]
        dataset with rounded values

    Raises
    ------
    ValueError
        If both binary_bits and decimal_digits are used
    ValueError
        If neither binary_bits or decimal_digits are used
    """
    if binary_bits:
        bits = binary_bits
        if decimal_digits:
            raise ValueError(
                f"Only one of binary_bits and decimal_digits can be used. Given {binary_bits} and {decimal_digits}, respectively."
            )
    elif decimal_digits:
        bits = np.ceil(decimal_digits * np.log(10) / np.log(2))
    else:
        raise ValueError(
            f"One of binary_bits and decimal_digits must be an integer. Given {binary_bits} and {decimal_digits}, respectively."
        )

    multiplier = 2 ** bits
    divisor = 2 ** (
        -1 * bits
    )  # faster multiply by reciprocal than to divide, according to some StackOverflow post. Probably doesn't matter much.
    return (ds * multiplier).round() * divisor


def chunk_transformations(ds: xr.Dataset) -> None:
    """Column transformations for MERRA-2 data.
    * Change temperature units from K to degrees C
    * Convert wind vector components to magnitude and direction (degrees in [0,360] from North, positive going clockwise)
    * Log10 transform precipitation
    * Remove unused columns

    Parameters
    ----------
    ds : xr.Dataset
        MERRA-2 dataset

    Returns
    -------
    None
        Modifications are IN PLACE
    """
    ds.update(ds[["TS", "T10M"]] - 273.15)  # convert units K -> C
    ds["WS50M"] = np.sqrt(np.square(ds["V50M"]) + np.square(ds["U50M"]))
    ds["WDIR50M"] = np.mod(
        np.arctan2(ds["U50M"], ds["V50M"]) + 2 * np.pi, 2 * np.pi
    ) * (
        180 / np.pi
    )  # This gives angle from North, positive going clockwise
    ds["PRECTOTCORR"] = np.log10(ds["PRECTOTCORR"] + 2 ** -48)
    return ds.drop_vars(["U50M", "V50M", "Z0M"])


def reduce_precision(ds: xr.Dataset, fp16=False) -> None:
    """Remove false precision IN PLACE to facilitate downstream compression. Two methods are conversion to float16 and back (3.3 decimal digits precision) and binary rounding to fixed precision. The appropriate levels were determined in './data/merge and rechunk.ipynb'. **Hardcoded** for current iteration of MERRA-2 data.

    Parameters
    ----------
    ds : xr.Dataset
        dataset of MERRA-2
    fp16 : bool, optional
        If True, use fp16 conversion method. If False, use fixed precision rounding method. By default False
    """
    ds[["lat", "lon"]].astype(np.float32)
    ds["PS"] = binary_round(ds["PS"], decimal_digits=-1).astype(np.int32)
    mask = ds["PRECTOTCORR"] <= -14  # assumes log10 applied first!
    ds["PRECTOTCORR"] = xr.where(
        mask, 0, ds["PRECTOTCORR"]
    )  # threshold tiny floats to 0
    if fp16:
        f16 = ["GHLAND", "RHOA", "PRECTOTCORR", "RISFC", "TS", "T10M"]
        ds.update(ds[f16].astype(np.float16).astype(np.float32))
        for dec, cols in {1: ["WDIR50M"], 3: ["WS50M"]}:
            ds.update(binary_round(ds[cols], decimal_digits=dec))
    else:
        prec = {
            1: ["GHLAND", "RISFC", "TS", "T10M", "WDIR50M"],
            2: ["PRECTOTCORR"],
            3: ["RHOA", "WS50M"],
        }
        for dec, cols in prec.items():
            ds.update(binary_round(ds[cols], decimal_digits=dec))


def make_divisions(
    ds: xr.Dataset,
    points_per_partition: Optional[int] = None,
    mb_per_partition: Optional[int] = None,
) -> list:
    """Calculate partition divisions for converting an xr.Dataset to a dask.DataFrame given either points per partition or megabytes per partition. Form is list(0, step size, 2* step size, ... , last index value) """
    time = ds.time.size
    total_points = len(ds.lat) * len(ds.lon)
    if points_per_partition:
        if mb_per_partition:
            raise ValueError(
                f"Choose only one of points_ or mb_ per_partition. Given {points_per_partition} and {mb_per_partition}, respectively"
            )
        step_size = time * points_per_partition
        steps = total_points // points_per_partition
        return [step_size * i for i in range(steps)] + [time * total_points - 1]

    elif mb_per_partition:
        mb_per_point = ds.nbytes / total_points / 2 ** 20
        points = mb_per_partition // mb_per_point
        return make_divisions(ds, points_per_partition=points)

    else:
        raise ValueError(
            f"Must choose one of points_ or mb_ per_partition. Given {points_per_partition} and {mb_per_partition}, respectively"
        )
