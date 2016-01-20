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
usage: maxpacker.py [-h] [-o OUTPUT] [-i INDEX] [-n NAME] [-f FORMAT]
                    [--p7z-args P7Z_ARGS] [--p7z-cmd P7Z_CMD]
                    [--tar-sort {0,1,2,3}] [-r ROOT] [--totalsize TOTALSIZE]
                    [-m MAXFILESIZE] [--minfilesize MINFILESIZE] [-e EXCLUDE]
                    [--include INCLUDE] [--exclude-re EXCLUDE_RE]
                    [--include-re INCLUDE_RE] [-a AFTER] [-b BEFORE]
                    [-s MAXPARTSIZE] [--maxfilenum MAXFILENUM] [-p PART]
                    PATH [PATH ...]

A flexible backup tool.

positional arguments:
  PATH                  Paths to archive

optional arguments:
  -h, --help            show this help message and exit

Output:
  output control

  -o OUTPUT, --output OUTPUT
                        output location
  -i INDEX, --index INDEX
                        index file
  -n NAME, --name NAME  output file/folder name format (Default: %03d[.ext])
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

  -r ROOT, --root ROOT  relative path root (Default: the longest prefix of all
                        paths)
  --totalsize TOTALSIZE
                        total size limit
  -m MAXFILESIZE, --maxfilesize MAXFILESIZE
                        max size of each file
  --minfilesize MINFILESIZE
                        min size of each file
  -e EXCLUDE, --exclude EXCLUDE
                        exclude files that match the rsync-style pattern
  --include INCLUDE     include files that match the rsync-style pattern
  --exclude-re EXCLUDE_RE
                        exclude files that match the regex pattern
  --include-re INCLUDE_RE
                        include files that match the regex pattern
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

  -s MAXPARTSIZE, --maxpartsize MAXPARTSIZE
                        max partition size
  --maxfilenum MAXFILENUM
                        max file number per partition
  -p PART, --part PART  partition number (overrides: -s, --maxfilenum)
```

License
-------
MIT License.
