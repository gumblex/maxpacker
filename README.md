Maxpacker
=========

Pack files into independent compressed volumes by predicting compressed size of files.

Using fpart's algorithm to allocate files.

It only supports 7z now, but it can be improved to support zip, tar.*z, etc. Because there isn't a Python 7z library for writing, so the compressed data in predicting is not efficiently used.

Usage
-----

```
usage: maxpacker.py [-h] [-s MAXSIZE] [-n] [-o PREFIX] FOLDER

Independent compressed volumes maxium packer.

positional arguments:
  FOLDER                Folder to archive

optional arguments:
  -h, --help            show this help message and exit
  -s MAXSIZE, --maxsize MAXSIZE
                        max volume size
  -n, --dry-run         only allocate files
  -o PREFIX, --prefix PREFIX
                        output files prefix
```

License
-------
The program itself is released into Public Domain.

The `eta.py` is licensed under BSD 3-clauses. see `LICENSE-eta`.
