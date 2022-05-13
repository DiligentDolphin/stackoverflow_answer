import os
import fnmatch
import logging
import pandas as pd


def file_filter(subdir, pattern=None):
    """Filter files match glob pattern in subdir"""
    all_files = os.listdir(subdir)
    # filter return a list of filename match pattern,
    # but not includes subdir
    match_files = fnmatch.filter(all_files, pattern)
    return match_files


def join(root, file_list):
    """Join root path to file_list"""
    _join = lambda x, *args: os.path.normpath(os.path.join(x, *args))
    return [_join(root, x) for x in file_list]


def load(io, *, **kwargs):
    """Load csv using pd.read_csv
    
    You may includes other jobs about load here, e.g.:
    - string strip whitespace
    - convert string to number
    """
    return pd.read_csv(io, **kwargs)


def loads(fp, *, **kwargs):
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
    

def concat(root, files, kwargs_read_csv):
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
        zip(
            [last_dir] * len_files,  # manual boardcast
            files,
            datestamp_list
        ),
        names=['root', 'file', 'date']
    )

    # concat
    full_path = join(root, files)
    dfs = pd.concat(
        loads(full_path, **kwargs_read_csv),
        index=mi,
    )
    return dfs


def main():
    """Main code"""

    # parameter, maybe later pass-in as arguments
    path_new = r"C:\\Users\\Bilal\\Python\\Task1\\NewVersionFiles\\"
    path_old = r"C:\\Users\\Bilal\\Python\\Task1\\OlderVersionFiles\\"
    pattern = "*.csv"           # glob pattern for fnmatch to filter

    # filter files
    files_new = file_filter(path_new, pattern)
    files_old = file_filter(path_old, pattern)

    # load csv and concat
    read_csv__kwargs = {'index_col': None, 'header': None}
    dfs_new = concat(path_new, files_new, read_csv__kwargs)
    dfs_old = concat(path_old, files_old, read_csv__kwargs)    

    # compare
    # use lazy compare method:
    compare_obj = zip(dfs_new, dfs_old)
