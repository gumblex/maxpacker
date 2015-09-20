#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import zlib
import lzma
import fnmatch
import logging
import operator
import subprocess

__version__ = '2.0'

_ig1 = operator.itemgetter(1)
_psize = operator.attrgetter('size')

logging.basicConfig(stream=sys.stdout, format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

def splitpath(path):
    '''
    Splits a path to a list.
    >>> p = splitpath('a/b/c/d/')
    # p = ['a', 'b', 'c', 'd']
    >>> p = splitpath('/a/b/c/d')
    # p = ['/', 'a', 'b', 'c', 'd']
    '''
    folders = []
    path = path.rstrip(r'\\').rstrip(r'/')
    while 1:
        path,folder = os.path.split(path)
        if folder != "":
            folders.append(folder)
        else:
            if path != "":
                folders.append(path)
            break
    folders.reverse()
    return folders

def human2bytes(s):
	"""
	>>> human2bytes('1M')
	1048576
	>>> human2bytes('1G')
	1073741824
	"""
	try:
		return int(s)
	except ValueError:
		symbols = ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
		letter = s[-1:].strip().upper()
		num = s[:-1]
		num = float(num)
		prefix = {symbols[0]: 1}
		for i, s in enumerate(symbols[1:]):
			prefix[s] = 1 << (i+1)*10
		return int(num * prefix[letter])

class Volume:
    def __init__(self, ffilter=None, compressfunc=None):
        self.ffilter = ffilter or TrueFilter()
        self.compressfunc = compressfunc

    def scanpaths(self, paths):
        parentdir = os.path.join(*os.path.commonprefix(tuple(map(splitpath, map(os.path.abspath, paths)))))
        fl = []
        ignored = []
        logging.info("Scanning files...")
        for path in paths:
            if os.path.isfile(path):
                try:
                    if self.ffilter(path):
                        fl.append((path, os.path.getsize(path)))
                    else:
                        ignored.append(path)
                except Exception as ex:
                    logging.exception("Can't access " + path)
            else:
                for root, dirs, files in os.walk(path):
                    for name in files:
                        fn = os.path.join(root, name)
                        try:
                            if self.ffilter(fn):
                                fl.append((fn, os.path.getsize(fn)))
                            else:
                                ignored.append(fn)
                        except Exception as ex:
                            logging.exception("Can't access " + fn)
        logging.info("## Ignored files:")
        for fn in ignored:
            print(fn)
        logging.info("## End list.")
        del ignored
        # estimate compressd size
        if callable(self.compressfunc):
            logging.info("Calculating estimated compressed size...")
            eta = ETA(len(fl), min_ms_between_updates=500)
            for k, v in enumerate(fl):
                filename, size = v
                try:
                    fl[k] = (filename, self.estcompresssize(filename, size))
                except Exception as ex:
                    logging.exception("Can't access " + path)
                eta.print_status()
            eta.done()
        self.filelist = fl

    def estcompresssize(self, filename, fsize, err=0.1):
        if not fsize:
            return 0
        if fsize <= self.samplesize:
            with open(filename, 'rb') as f:
                sample = f.read()
        else:
            with open(filename, 'rb') as f:
                f.seek(int((fsize - self.samplesize)/2))
                sample = f.read(self.samplesize)
        compsize = len(self.compressfunc(sample)) / len(sample)
        return int(fsize * compsize / (1 + err))

# Composition support magic from Whoosh

class Composable:
    def __or__(self, other):
        assert callable(other), "%r is not callable" % other
        return CompositeFilter(self, other)

    def __repr__(self):
        attrs = ""
        if self.__dict__:
            attrs = ", ".join("%s=%r" % (key, value) for key, value in self.__dict__.items())
        return self.__class__.__name__ + "(%s)" % attrs

# Various filters to use before packing

class Filter(Composable):
    def __eq__(self, other):
        return other and self.__class__ is other.__class__ and self.__dict__ == other.__dict__

    def __call__(self, value, **kwargs):
        raise NotImplementedError

class CompositeFilter(Filter):
    def __init__(self, *composables):
        self.items = []
        for comp in composables:
            if isinstance(comp, CompositeFilter):
                self.items.extend(comp.items)
            else:
                self.items.append(comp)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__,
                           ", ".join(repr(item) for item in self.items))

    def __call__(self, filename):
        items = self.items
        gen = items[0](filename)
        for item in items[1:]:
            gen = gen and item(filename)
        return gen

    def __getitem__(self, item):
        return self.items.__getitem__(item)

    def __len__(self):
        return len(self.items)

    def __eq__(self, other):
        return other and self.__class__ is other.__class__ and self.items == other.items

class TrueFilter(Filter):
    '''
    Always returns True.
    '''
    def __call__(self, filename):
        return True

class GlobFilter(Filter):
    '''
    Select files matching with shell patterns.
    `include` and `exclude` must be two lists of patterns.
    An empty list means include all.
    '''

    def __init__(self, include=(), exclude=()):
        self.include = include or ('*',)
        self.exclude = exclude

    def __call__(self, filename):
        return (
            any(fnmatch.fnmatch(filename, pat) for pat in self.include) and not
            any(fnmatch.fnmatch(filename, pat) for pat in self.exclude))

class RegexFilter(Filter):
    '''
    Select files matching with regex patterns.
    Note that it uses `match` not `search`.
    `include` and `exclude` must be two lists of patterns.
    An empty list means include all.
    '''

    def __init__(self, include=(), exclude=()):
        self.include = tuple(re.compile(r) for r in include or ('',))
        self.exclude = tuple(re.compile(r) for r in exclude)

    def __call__(self, filename):
        return (
            any(pat.match(filename) for pat in self.include) and not
            any(pat.match(filename) for pat in self.exclude))

class SizeFilter(Filter):
    '''
    Select files which is smaller than or equal to `maxsize`.
    If None, it selects all.
    '''

    def __init__(self, maxsize=None):
        self.maxsize = maxsize

    def __call__(self, filename):
        return self.maxsize is None or os.path.getsize(filename) <= self.maxsize

class TimeFilter(Filter):
    '''
    Select files which is in the range between `mintime` and `maxtime`.
    (min and max value included.)
    None means not specified. `timetype` must be one of the three values:
    * 'm' for the time of last modification (most reliable)
    * 'c' for the time of the last metadata change or the creation time
    * 'a' for the time of last access
    The time is a number giving the number of seconds since the epoch.
    '''

    def __init__(self, mintime=None, maxtime=None, timetype='m'):
        self.mintime = mintime
        self.maxtime = maxtime
        if timetype == 'm':
            self.gettime = os.path.getmtime
        elif timetype == 'c':
            self.gettime = os.path.getctime
        elif timetype == 'a':
            self.gettime = os.path.getatime
        else:
            raise ValueError("`timetype` must be one of 'm', 'c', 'a'")

    def __call__(self, filename):
        return ((self.mintime is None or self.gettime(filename) >= self.mintime)
            and (self.maxtime is None or self.gettime(filename) <= self.maxtime))

# Packing methods

class Partition:
    def __init__(self):
        self.filelist = []
        self.size = 0

    def __repr__(self):
        return "<Partition size=%r numfiles=%r>" % (self.size, self.numfiles)

    def __len__(self):
        return len(self.filelist)

    def __iter__(self):
        return iter(self.filelist)

    def __getitem__(self, key):
        return self.filelist[key]

    def __bool__(self):
        return bool(self.filelist)

    def addfile(self, filename, size):
        self.filelist.append(filename, size)
        self.size += size

class PackerBase:
    def __repr__(self):
        attrs = ""
        if self.__dict__:
            attrs = ", ".join("%s=%r" % (key, value) for key, value in self.__dict__.items())
        return self.__class__.__name__ + "(%s)" % attrs

    def dispatch(self, filelist):
        raise NotImplementedError

class SingleVolumePacker(PackerBase):
    def dispatch(self, filelist):
        part = Partition()
        for filename, size in filelist:
            part.addfile(filename, size)
        return [part]

class LimitPacker(PackerBase):
    def __init__(self, maxsize=0, maxentries=0, multipart=True):
        self.maxsize = maxsize
        self.maxentries = maxentries
        self.multipart = multipart

    def dispatch(self, filelist):
        partitions = self.single_dispatch(filelist, self.maxsize, self.maxentries)
        # efficiently split large files (in Partition 0) across partitions
        if self.multipart:
            multipart = 1
            while partitions[0]:
                multipart += 1
                temppart = self.single_dispatch(partitions[0].filelist + partitions.pop().filelist, self.maxsize*multipart, self.maxentries)
                partitions[0] = temppart[0]
                partitions.extend(filter(None, temppart[1:]))
            partitions.pop(0)

    def single_dispatch(self, filelist, maxsize=0, maxentries=0):
        if maxsize:
            # when maxsize is used, create a default partition (Partition 0)
            #   that will hold files that does not match criteria
            partitions = [Partition(), Partition()]
            pn = startp = 1
        else:
            partitions = [Partition()]
            pn = startp = 0
        for filename, size in filelist:
            if 0 < maxsize < size:
                partitions[0].addfile(filename, size)
            else:
                # examine each partition
                while pn < len(partitions):
                    # if file does not fit in partition
                    if ((maxentries > 0) and (len(partitions[pn]) + 1 > maxentries)) or ((maxsize > 0) and (partitions[pn].size + size > maxsize)):
                        # and we reached last partition, chain a new one
                        if pn == len(partitions) - 1:
                            partitions.append(Partition())
                        # examine next partition
                        pn += 1
                    else:
                        # file fits in current partition, add it
                        partitions[pn].addfile(filename, size)
                        # examine next file
                        break
            # examine next file
            # come back to the first partition
            pn = startp
        return partitions

class PartNumberLimitPacker(PackerBase):
    def __init__(self, numentries):
        self.numentries = numentries

    def dispatch(self, filelist):
        # our list of partitions
        partitions = [Partition() for i in range(self.numentries)]
        # sort files with a fixed size of partitions
        filelist = filelist.sort(key=_ig1, reverse=True)
        emptyfiles = []
        # dispatch files
        for filename, size in filelist:
            if size > 0:
                # find most approriate partition
                part = min(partitions, key=_psize)
                # assign it and load the partition with file size
                part.addfile(filename, size)
            else:
                emptyfiles.append(filename)
        # re-dispatch empty files
        self.spreadfiles(partitions, emptyfiles)
        return partitions

    def spreadfiles(self, partitions, filelist):
        fpp, rem = divmod(len(filelist), len(partitions))
        n = 0
        for part in partitions:
            for i in range(fpp):
                part.addfile(filelist[n])
                n += 1
        for i in range(rem):
            part.addfile(filelist[n])
            n += 1
        assert n == len(filelist)

class CopyOutput:
    pass

class LinkOutput:
    pass

class SubprocessOutput:
    pass
