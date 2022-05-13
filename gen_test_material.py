import numpy as np
import pandas as pd

"""Generate test material for example code"""


def isoformat_date(ts):
    """return isoformat with date only"""
    return ts.isoformat().split("T")[0]


def main(
    kwargs_new_date_range=None,
    kwargs_old_date_range=None,
):
    ## get filename from DatetimeIndex
    date_range_new = pd.date_range(**kwargs_new_date_range)
    date_range_old = pd.date_range(**kwargs_old_date_range)
    filenames_new = [f"{isoformat_date(ts)}.csv" for ts in date_range_new]
    filenames_old = [f"{isoformat_date(ts)}.csv" for ts in date_range_old]
    
    print(filenames_new)
    ## create content for each file
    
    ## write disk

    return date_range_new


def test():
    kwargs_new_date_range = {
        "start": "2000-01-01",
        "end": "2022-12-31",
        "freq": "M",
    }
    kwargs_old_date_range = {
        "start": "1999-01-01",
        "end": "2021-12-31",
        "freq": "M",
    }
    main(
        kwargs_new_date_range=kwargs_new_date_range,
        kwargs_old_date_range=kwargs_old_date_range,
    )
    return


if __name__ == "__main__":
    test()
