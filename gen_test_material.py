import os
import string
import numpy as np
import pandas as pd

"""Generate test material for example code"""


def _join(path, *paths):
    return os.path.normpath(os.path.join(path, *paths))


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
    array = np.ndarray(shape, dtype="float")

    # create value
    value = np.array([np.random.random() for x in range(array.size)])
    value = value.reshape(array.shape)

    return value


def create_write_csv(filename, ascii_uppercase=None):
    ## create content for each file
    # columns for data
    if not ascii_uppercase:
        ascii_uppercase = list(string.ascii_uppercase)
    data = create_random_array()
    columns = ascii_uppercase[:data.shape[1]]
    index = pd.RangeIndex(data.shape[0])
    df = pd.DataFrame(data, index=index, columns=columns)

    ## write disk
    try:
        df.to_csv(filename)
    except OSError:
        os.makedirs(os.path.split(filename)[0])
        df.to_csv(filename)
    return df


def write_csv_files(root, filenames, ascii_uppercase=None):
    for filename in filenames:
        fp = _join(root, filename)
        create_write_csv(fp, ascii_uppercase=ascii_uppercase)


def main(
    root_new,
    root_old,
    kwargs_new_date_range=None,
    kwargs_old_date_range=None,
):
    ascii_uppercase = list(string.ascii_uppercase)

    ## get filename from DatetimeIndex
    date_range_new = pd.date_range(**kwargs_new_date_range)
    date_range_old = pd.date_range(**kwargs_old_date_range)
    filenames_new = [f"{isoformat_date(ts)}.csv" for ts in date_range_new]
    filenames_old = [f"{isoformat_date(ts)}.csv" for ts in date_range_old]

    write_csv_files(root_new, filenames_new, ascii_uppercase=ascii_uppercase)
    write_csv_files(root_old, filenames_old, ascii_uppercase=ascii_uppercase)

    return


def test():
    root_new = 'dist/NewVersion'
    root_old = 'dist/OldVersion'
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
        root_new,
        root_old,
        kwargs_new_date_range=kwargs_new_date_range,
        kwargs_old_date_range=kwargs_old_date_range,
    )
    return


def tmp_test():
    data = create_random_array()


if __name__ == "__main__":
    test()
