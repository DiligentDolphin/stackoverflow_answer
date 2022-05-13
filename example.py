import fnmatch
import json
import logging
import os

from copy import copy
from os import PathLike
from typing import Sequence
from typing import Union
from typing import Dict
from typing import Set
from numpy import reshape

import pandas as pd
import numpy as np


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


def json_serialization(obj, ensure_ascii=False):
    """
    Serialize using json.dumps()

    Note: set()
    TypeError: Object of type set is not JSON serializable
        This code only convert the first structure from set to list,
        if there is nesting element using set, json.dumps raise.
    """
    logger = logging.getLogger(__name__)

    _obj = copy(obj)

    # Convert Sequence to list, as json.loads return list
    if isinstance(_obj, (Set, Sequence)):
        _obj = list(_obj)
    _str = json.dumps(_obj, ensure_ascii=ensure_ascii)

    try:
        _restore = json.loads(_str)
        assert _obj == _restore
    except AssertionError as e:
        # The serialization is possiably not reversible
        logger.warning(f"Serialization of {obj} may not be reversible.")

    return _str


def standarize_index(index_obj):
    """
    standarize MultiIndex by flatten to series

    Here use pd.Index.to_flat_index() to flatten MultiIndex, then use json.dumps()
    serialize to string
    """
    # check is MultiIndex
    if index_obj.nlevels == 1:
        index_1_D = index_obj.copy()
    else:
        # flatten Index
        index_flat = index_obj.to_flat_index()
        # serialize
        index_serialized = (json_serialization(o) for o in index_flat)
        # convert back to 1-D index
        # apply dtype to 'category' saving storage space
        index_1_D = pd.Index(index_serialized, dtype="category")
    return index_1_D


def standarize_df(df, name="value"):
    """
    standarize MultiIndex by flatten to series

    Here use pd.Index.to_flat_index() to flatten MultiIndex, then use json.dumps()
    serialize to string
    """
    # if df already Series, skip convert columns
    if isinstance(df, pd.Series):
        _column_std = None
    else:
        _column_std = standarize_index(df.columns)

    _index_std = standarize_index(df.index)
    _value = df.values

    # index standarize
    _index_standarized_df = pd.DataFrame(
        data=_value, index=_index_std, columns=_column_std
    )

    # reshape
    _reshape_df = _index_standarized_df.stack()
    _reshape_df.index.names = ["index", "column"]

    # name the series
    # pd.merge cannot merge 2 series with no name
    _reshape_df.name = name

    err_return_type_not_series = (
        f"Return type of standarize_df is {type(_reshape_df)}, expect pd.Series."
    )
    # ensure return Series
    assert isinstance(_reshape_df, pd.Series), err_return_type_not_series

    return _reshape_df


def _compare_df_parse_general(df_new, df_old):
    """General method to compare arbitrarily DataFrame by column and index

    road map:
    1. standarize MultiIndex by flatten to series
        Here use pd.Index.to_flat_index() to flatten MultiIndex, then use json.dumps()
        serialize to string
    2. reshape 2-D DataFrame to 1-D Series
        With following pattern: Column, Index, Value
    3. compare the standarized Series
    """
    logger = logging.getLogger(__name__)

    df_new_std = standarize_df(df_new)
    df_old_std = standarize_df(df_old)

    # join, outer method
    df_join = pd.merge(
        df_new_std,
        df_old_std,
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=("_new", "_old"),
        indicator="_source",
    )

    # compare already complete, the rest is filter result
    # assumption: 
    #    if new = old, pass silently
    #    if new only (left_only), report new
    #    if old only (right_only), report del
    #    if new <> old, report change
    
    def filter_result(df):
        for (index, value) in df_join.iterrows():
            index_str, column_str = index
            value_new, value_old = value[0:2]
            if value_new == value_old:
                pass
            elif value_old is np.NaN and value_new:
                ret = (index, value)
                log = (
                    f"New Value create:\n"
                    f"   Column: {column_str!r}"
                    f"   Row: {index_str!r}"
                    f"   Value: {value_new!r}"
                )
            elif value_new is np.NaN and value_old:
                ret = (index, value)
                log = (
                    f"Old Value create:\n"
                    f"   Column: {column_str!r}"
                    f"   Row: {index_str!r}"
                    f"   Value: {value_old!r}"
                )
            else:
                ret = (index, value)
                log = (
                    f"Value updated:\n"
                    f"   Column: {column_str!r}"
                    f"   Row: {index_str!r}"
                    f"   NewValue: {value_new!r}"
                    f"   OldValue: {value_old!r}"
                )
            logging.debug(log)
            yield ret

    # unzip x by: zip(*x)
    index_value_pair = filter_result(df_join)
    compare_result = pd.DataFrame.from_records(zip(*index_value_pair)).T

    return compare_result


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
    filename_alter: Union[PathLike, None] = None,
    kwargs_read_csv: Union[Dict, None] = None,
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
