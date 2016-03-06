Maxpacker
=========

A flexible backup tool.

Features
---------

* Filter files by file name, modification time and size
* Pack files into independent partitions by size or number of files or partitions using the algorithms from [fpart](https://github.com/martymac/fpart)
* Backup the splitted partitions with copy/link/7z/zip/tar.*z
* Predict the final compressed file size and pack efficiently

Usage
-----

```
usage: maxpacker.py [-h] [-o DIR] [-i FILE] [-n PATTERN] [-f FORMAT]
                    [--p7z-args P7Z_ARGS] [--p7z-cmd P7Z_CMD]
                    [--tar-sort {0,1,2,3}] [-r DIR] [--totalsize TOTALSIZE]
                    [-m SIZE] [--minfilesize SIZE] [-e PATTERN]
                    [--exclude-from FILE] [--include PATTERN]
                    [--include-from FILE] [--exclude-re PATTERN]
                    [--exclude-re-from FILE] [--include-re PATTERN]
                    [--include-re-from FILE] [-a AFTER] [-b BEFORE] [-s SIZE]
                    [--maxfilenum NUM] [-p NUM]
                    PATH [PATH ...]

A flexible backup tool.

positional arguments:
  PATH                  Paths to archive

optional arguments:
  -h, --help            show this help message and exit

Output:
  output control

  -o DIR, --output DIR  output location
  -i FILE, --index FILE
                        index file
  -n PATTERN, --name PATTERN
                        output file/folder name format (Default: %03d[.ext])
  -f FORMAT, --format FORMAT
                        output format, can be one of 'none', 'copy', 'link',
                        '7z', 'zip', 'tar', 'tar.gz', 'tar.bz2', 'tar.xz'
                        (Default: 7z)
  --p7z-args P7Z_ARGS   extra arguments for 7z (only for -f 7z) (TIP: use
                        --p7z-args='-xxx' to avoid confusing the argument
                        parser)
  --p7z-cmd P7Z_CMD     7z program to use (Default: 7za, only for -f 7z)
  --tar-sort {0,1,2,3}  sort file in a partition (only for -f tar.*z). 0: no
                        sort, 1: normal sort, 2(default): 7z-style sort within
                        a directory, 3: 7z-style sort within a partition.

Filter:
  options for filtering files

  -r DIR, --root DIR    relative path root (Default: the longest prefix of all
                        paths)
  --totalsize TOTALSIZE
                        total size limit
  -m SIZE, --maxfilesize SIZE
                        max size of each file
  --minfilesize SIZE    min size of each file
  -e PATTERN, --exclude PATTERN
                        exclude files that match the rsync-style pattern
  --exclude-from FILE   read exclude patterns from FILE, one pattern per line.
                        Ignore completely empty lines.
  --include PATTERN     include files that match the rsync-style pattern
  --include-from FILE   read include patterns from FILE, one pattern per line.
                        Ignore completely empty lines.
  --exclude-re PATTERN  exclude files that match the regex pattern
  --exclude-re-from FILE
                        read exclude regexes from FILE, one regex per line.
                        Ignore completely empty lines.
  --include-re PATTERN  include files that match the regex pattern
  --include-re-from FILE
                        read include regexes from FILE, one regex per line.
                        Ignore completely empty lines.
  -a AFTER, --after AFTER
                        select files whose modification time is after this
                        value (Format: %Y%m%d%H%M%S, eg. 20140101120000, use
                        local time zone)
  -b BEFORE, --before BEFORE
                        select files whose modification time is before this
                        value (Format: %Y%m%d%H%M%S, eg. 20150601000000, use
                        local time zone)

Partition:
  partition methods

  -s SIZE, --maxpartsize SIZE
                        max partition size
  --maxfilenum NUM      max file number per partition
  -p NUM, --part NUM    partition number (overrides: -s, --maxfilenum)
```

License
-------
MIT License.
