import os
import fnmatch
import pandas as pd


def file_filter(subdir, pattern=None):
    """Filter files match glob pattern in subdir"""
    join = lambda x, *args: os.path.normpath(os.path.join(x, *args))
    all_files = os.listdir(subdir)
    # filter return a list of filename match pattern,
    # but not includes subdir
    match_files = fnmatch.filter(all_files, pattern)
    # append subdir
    match_full_path = [join(subdir, x) for x in match_files]
    return match_files, match_full_path


def load(io, *, **kwargs):
    """Load csv using pd.read_csv"""
    return pd.read_csv(io, **kwargs)


def to_timestamp(s):
    """Convert string to datetime stamp
    
    pd.to_datetime() accept standard ISO datetime string format,
       yyyy-mm-dd hh:mm:ss
    hh:mm:ss is optional.
    if this is not the case, either convert string, or using
    datetime module construct directly.
    """
    return pd.to_datetime(s)
    

def concat():
    """Concat dataframes

    Index:
    In each versionFolder, your csv file has datetime info in their
    filename, convert them to timestamp using function `to_timestamp`

    Frame:
    using function `load` to load csv file to dataframes
    """
    

def main():
    """Main code"""
    # filter files
    path_new = r""
    path_old = r""
    pattern = "*.csv"           # glob pattern for fnmatch to filter
    files_new, fps_new = file_filter(path_new, pattern)
    files_old, fps_old = file_filter(path_old, pattern)

    # convert filenames to timestamp
    ts_new = list(map(to_timestamp, files_new))
    ts_old = list(map(to_timestamp, files_old))

    # convert DatetimeIndex
    di_new = pd.DatetimeIndex(ts_new, name='date')
    di_old = pd.DatetimeIndex(ts_old, name='date')
    
    # load csv
    read_csv__kwargs = {'index_col': None, 'header': None}
    dfs_new = pd.concat(
        (load(x, **read_csv__kwargs) for x in fps_new),
        index=di_new,
        ignore_index=True,
    )
    dfs_old = pd.concat(
        (load(x, **read_csv__kwargs) for x in fps_old),
        index=di_old,
        ignore_index=True,
    )

    # compare
    # index structure should be like this
    #    nlevel[0]: datetime from your csv filenames
    # column structure should be same as your first row in csv files
    #
    # iter throught both dataframes, using the following structure
    # rather then nesting for, which resulting n * n loops

    group_new = dfs_new.groupby('date')
    group_old = dfs_old.groupby('date')

    for (idx_new, subdf_new), (idx_old, subdf_old) in zip(group_new, group_old):
        # As your description, I assumed your csv files in OlderVersion and
        # NewVersion has same name, thus:
        #    idx_new == subdf_new
        # if this is not the case, provides the relation between them, and
        # to make them have same index while concat

        # ensure you are comparing same file from two versionFolder
        assert idx_new == idx_old
