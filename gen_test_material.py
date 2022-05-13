import numpy as np
import pandas as pd

"""Generate test material for example code"""


def isoformat_date(ts):
    """return isoformat with date only"""
    return ts.isoformat().split("T")[0]



def get_rand_shape(x_min, x_max, y_min, y_max):
    x = np.random.randint(x_min, x_max)
    y = np.random.randint(y_min, y_max)
    return (x, y)


def create_random_array(shape=None, random_shape_range=None):
    """Create random shape float data"""
    if shape:
        shape = shape
    elif random_shape_range:
        shape = get_rand_shape(*random_shape_range)
    else:
        random_shape_range = (55, 76, 8, 14)
        shape = get_rand_shape(*random_shape_range)

    # create array
    array = np.ndarray(shape, dtype='float')

    # create value
    value = np.array([np.random.random() for x in range(array.size)])
    value = value.reshape(array.shape)

    return value


def main(
    kwargs_new_date_range=None,
    kwargs_old_date_range=None,
):
    ## get filename from DatetimeIndex
    date_range_new = pd.date_range(**kwargs_new_date_range)
    date_range_old = pd.date_range(**kwargs_old_date_range)
    filenames_new = [f"{isoformat_date(ts)}.csv" for ts in date_range_new]
    filenames_old = [f"{isoformat_date(ts)}.csv" for ts in date_range_old]

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


def tmp_test():
    data = create_random_array()


if __name__ == "__main__":
    tmp_test()
