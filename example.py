import os
import fnmatch
import logging
import pandas as pd

from typing import (
    Union,
    Sequence,
    Dict,
    PathLike,
)


def _join(path, *paths):
    return os.path.normpath(os.path.join(path, *paths))


def join(root, file_list):
    """Join root path to file_list"""
    return [_join(root, x) for x in file_list]


def file_filter(subdir, pattern=None):
    """Filter files match glob pattern in subdir"""
    all_files = os.listdir(subdir)
    # filter return a list of filename match pattern,
    # but not includes subdir
    match_files = fnmatch.filter(all_files, pattern)
    return match_files


def file_exists(filename, pathname, method=None):
    """Wrapper of os.path.exists

    Return bool if method is not 'error' and logging;
    Raise FileExistsError if method is 'error'.
    """
    logger = logging.getLogger(__name__)
    fp = _join(pathname, filename)
    exists = os.path.exists(fp)
    err_msg = f"{filename} not exists in {pathname}"
    if method == "error":
        raise FileExistsError(err_msg)
    else:
        logger.warning(err_msg)
        # pass warning message
        exists = err_msg
    return exists


def load(io, **kwargs):
    """Load csv using pd.read_csv

    You may includes other jobs about load here, e.g.:
    - string strip whitespace
    - convert string to number
    """
    return pd.read_csv(io, **kwargs)


def loads(fp, **kwargs):
    """wrapper to load from multiple files

    :fp: list of full_path to csv file
    """
    for f in fp:
        yield load(f, **kwargs)


def to_timestamp(s):
    """Convert string to datetime stamp

    pd.to_datetime() accept standard ISO datetime string format,
       yyyy-mm-dd hh:mm:ss
    hh:mm:ss is optional.
    if this is not the case, either convert string, or using
    datetime module construct directly.
    """
    return pd.to_datetime(s)


def make_multi_index(root, files, kwargs_read_csv):
    """Main loop for concat csv files from each Folder

    Jobs done here:
    - construct index for pd.concat()
    - return concated DataFrame

    :root: The root dir of files, in this case is path1, path2
    :files: The filenames, filter in advance
    :kwargs_read_csv: kwargs passed to pd.read_csv, store in dict
    """

    # construct index of concated DataFrame: Here I prefer to includes
    # (root, filename, datetime) pairs as concated DataFrame's index,
    # thus you may decide to use which of them later.
    #
    # The MultiIndex would have 3 levels:
    # - The root dir, represented by last part
    # - the filename
    # - the datetime information
    len_files = len(files)
    last_dir = os.path.split(root)[-1]
    datestamp_list = [to_timestamp(x) for x in files]
    mi = pd.MultiIndex.from_tuples(
        zip([last_dir] * len_files, files, datestamp_list),  # manual boardcast
        names=["root", "file", "date"],
    )

    return mi


def _compare_df_parse_same(df_new, df_old, df_diff):
    """If the frame are same in shape, index, columns, parse it in following format:
    Column:
    Index:
    new_value:
    old_value:"""
    for (column_name, series) in df_diff.iteritems():
        _diff_series_new = 0


def _compare_df(df_new, df_old):
    df_new_column = df_new.columns
    df_new_index = df_new.index
    df_new_shape = df_new.shape
    df_old_column = df_old.columns
    df_old_index = df_old.index
    df_old_shape = df_old.shape

    same_shape = bool(df_new_shape == df_old_shape)
    same_column = bool(df_new_column == df_old_column)
    same_index = bool(df_new_index == df_old_index)

    ## same shape / columns / index
    if same_column and same_index and same_shape:
        # direct compare
        df_diff = df_new == df_old
        parser = _compare_df_parse_same(df_new, df_old, df_diff)


def compare_file(
    filename: PathLike,
    path_new: PathLike,
    path_old: PathLike,
    filename_alter: Union(PathLike, None) = None,
    kwargs_read_csv: Union(Dict, None) = None,
):
    """Compare single file from 2 dirs"""
    logger = logging.getLogger(__name__)
    ## Exam file exist

    exists_new = file_exists(filename, path_new)
    exists_old = file_exists(filename, path_old)
    all_exists = all(exists_new, exists_old)

    if all_exists:
        ## do compare
        pass

    elif exists_new:
        ## only exists in path_new
        logger.warning(exists_new)
        ret = exists_new

    elif exists_old:
        ## only exists in path_old
        logger.warning(exists_old)
        ret = exists_old

    else:
        ## not exists everywhere
        err_msg = f"{filename} not exists in both dir"
        logger.warning(err_msg)
        ret = err_msg

    return ret


def main():
    """Main code"""

    ## parameter, maybe later pass-in as arguments

    path_new = r"C:\\Users\\Bilal\\Python\\Task1\\NewVersionFiles\\"
    path_old = r"C:\\Users\\Bilal\\Python\\Task1\\OlderVersionFiles\\"
    pattern = "*.csv"  # glob pattern for fnmatch to filter

    ## filter files

    files_new = file_filter(path_new, pattern)
    files_old = file_filter(path_old, pattern)

    ## load csv and concat

    # Since this is a file-to-file compare, and there is a lot files,
    # there is no need to load all of them at once.
    read_csv__kwargs = {"index_col": None, "header": None}

    # compare
    # use lazy compare method:
