# Your object

From my understanding, your objectives are:

-   Compare \`csv\` files in 2 version folders, all filenames are same;
-   filenames contain date information;
-   For each file:
    -   the first row of \`csv\` files is columns title;
    -   the rest rows are data to compare;
    -   their is no column can be used as an nature index.
-   The program should save result in file rather than print to screen;

# some reference

## Filter files by name pattern

Use fnmatch.filter(alist, glob~namepattern~) to return a list of name
with some glob pattern, i.e.

\`fnmatch.filter(os.listdir(), \'\*.csv\')\`

return all .csv file in current dir.

## Transfer string to date (pd.to~dataframe~)

You can use pandas general function to convert string or ymd interger to
date data. This general function from pandas accept several sequence
arguments as input:

-   sequence of int, ordered by \"year\", \"month\", \"day\" if a
    DataFrame is provided,
-   string of date in localized format to set your local from your
    platform default, import locale module and set as follow:

\`\`\` import locale locale.setlocale(locale.ALL=\'\') \`\`\`

## Nested \`for...\` Loop (causing slow)

When iter over 2 iterable with same size (length), you use a nested
\`for...\` structure, that costs n\*n times of loop. There\'s 574 csv
files in each version folder, so it use 574\*574 loop, this is main
cause of slow. Instead, use zip() to pair iterable with same size, then
iter on the new zip object, this costs n times of loop and save n\*(n-1)
times from nested loop.

\`\`\` for old, new in zip(olds, news): my~functocompare~(old, new) \#
loop body here \`\`\`

# example code
