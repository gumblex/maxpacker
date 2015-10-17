#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import statistics
import collections

_ig0 = lambda x: x[0]
_ig1 = lambda x: x[1]

def timestring(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return ('%dd' % d if d else '') + ('%dh' % h if h else '') + ('%dm' % m if m else '') + ('%ds' % s if s else '')


def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def scanpaths(paths):
    for path in paths:
        yield path
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for name in files:
                    yield os.path.join(root, name)
                for name in dirs:
                    yield os.path.join(root, name)

def stat(paths):
    # [file, dir, link, mount, error]
    nums = [0, 0, 0, 0, 0]
    # file size distribution
    filesize = []
    # file type by extension, count by number
    filetypec = collections.Counter()
    # file type by extension, count by size
    filetypes = collections.Counter()
    # file modification time distribution
    filetime = []
    for path in scanpaths(paths):
        try:
            isfile = os.path.isfile(path)
            nums[0] += isfile
            nums[1] += os.path.isdir(path)
            nums[2] += os.path.islink(path)
            nums[3] += os.path.ismount(path)
            if isfile:
                root, ext = os.path.splitext(path)
                filetypec[ext] += 1
                filetime.append(os.path.getmtime(path))
                fsize = os.path.getsize(path)
                filesize.append(fsize)
                filetypes[ext] += fsize
        except Exception:
            nums[4] += 1
    return nums, filesize, filetypec, filetypes, filetime

def output(nums, filesize, filetypec, filetypes, filetime):
    fsum = sum(filesize)
    print(('%s. %s' % (', '.join('%d %s' % vals for vals in filter(_ig1, zip(nums, ('files', 'directories', 'links', 'mount points', 'errors')))), '%s data.' % sizeof_fmt(fsum))).lstrip('. '))
    if not filesize:
        return
    if len(filesize) > 2:
        favg = fsum / len(filesize)
        stdev = statistics.pstdev(filesize, favg)
        print('File size: max %s, mean %s, median %s, stdev %s' % tuple(map(sizeof_fmt, (max(filesize), favg, statistics.median(filesize), stdev))))
        print(' µ+σ (68%): ' + sizeof_fmt(favg + stdev) +
              ', µ+2σ (95%): ' + sizeof_fmt(favg + stdev * 2))
        print('Modification time:')
        print(' min    ' + time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(min(filetime))))
        print(' max    ' + time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(max(filetime))))
        tavg = statistics.mean(filetime)
        print(' mean   ' + time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(tavg)))
        print(' median ' + time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(statistics.median(filetime))))
        print(' stdev  ' + timestring(statistics.pstdev(filetime, tavg)))
        print('File type by number:')
        mcomm = filetypec.most_common(5)
        count = sum(filetypec.values())
        print('\n'.join(' % 6s: %.2f%%' % (k or '<N/A>', v/count*100) for k, v in mcomm))
        print(' Others: %.2f%%' % ((count - sum(v for k, v in mcomm)) / count * 100))
        print('File type by size:')
        mcomm = filetypes.most_common(5)
        count = sum(filetypes.values())
        print('\n'.join(' % 6s: %.2f%%' % (k or '<N/A>', v/count*100) for k, v in mcomm))
        print(' Others: %.2f%%' % ((count - sum(v for k, v in mcomm)) / count * 100))

def main():
    output(*stat(sys.argv[1:]))

if __name__ == '__main__':
    main()
