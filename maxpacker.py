#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import bz2
import zlib
import lzma
import time
import shlex
import shutil
import fnmatch
import logging
import tarfile
import zipfile
import tempfile
import operator
import argparse
import subprocess

from eta import ETA

__version__ = '2.0'

_ig1 = operator.itemgetter(1)
_psize = operator.attrgetter('size')

DEFAULT_ENCODING = 'utf-8'
tarfile.ENCODING = DEFAULT_ENCODING

logging.basicConfig(stream=sys.stdout, format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

exts_ord = {e:i for i,e in enumerate(
'''7z xz lzma ace arc arj bz tbz bz2 tbz2 cab deb gz tgz ha lha lzh lzo lzx pak rar rpm sit zoo
zip jar ear war msi
3gp avi mov mpeg mpg mpe wmv
aac ape fla flac la mp3 m4a mp4 ofr ogg pac ra rm rka shn swa tta wv wma wav
swf
chm hxi hxs
gif jpeg jpg jp2 png tiff bmp ico psd psp
awg ps eps cgm dxf svg vrml wmf emf ai md
cad dwg pps key sxi
max 3ds
iso bin nrg mdf img pdi tar cpio xpi
vfd vhd vud vmc vsv
vmdk dsk nvram vmem vmsd vmsn vmss vmtm
inl inc idl acf asa h hpp hxx c cpp cxx rc java cs pas bas vb cls ctl frm dlg def
f77 f f90 f95
asm sql manifest dep
mak clw csproj vcproj sln dsp dsw
classf
bat cmd
xml xsd xsl xslt hxk hxc htm html xhtml xht mht mhtml htw asp aspx css cgi jsp shtml
awk sed hta js php php3 php4 php5 phptml pl pm py pyo rb sh tcl vbs
text txt tex ans asc srt reg ini doc docx mcw dot rtf hlp xls xlr xlt xlw ppt pdf
sxc sxd sxi sxg sxw stc sti stw stm odt ott odg otg odp otp ods ots odf
abw afp cwk lwp wpd wps wpt wrf wri
abf afm bdf fon mgf otf pcf pfa snf ttf
dbf mdb nsf ntf wdb db fdb gdb
exe dll ocx vbx sfx sys tlb awx com obj lib out o so
pdb pch idb ncb opt'''.split(), 1)}
exts_ord[''] = 0

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

basepath = lambda paths: os.path.join(*os.path.commonprefix(tuple(map(splitpath, map(os.path.abspath, paths)))))

def human2bytes(s):
    """
    >>> human2bytes('1M')
    1048576
    >>> human2bytes('1G')
    1073741824
    """
    if s is None:
        return None
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

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

class Volume:

    def __init__(self, packer, ffilter=None, indexfile='index.txt', output=None, compressfunc=None, sortfile=0):
        self.packer = packer
        self.ffilter = ffilter or TrueFilter()
        self.indexfile = indexfile or os.devnull
        self.output = output or OutputBase()
        self.compressfunc = compressfunc
        self.sortfile = sortfile
        self.samplesize = 1024

    def run(self, paths, basedir=None):
        self.output.output(self.partition(paths, basedir=None))
        logging.info("Done.")

    def partition(self, paths, basedir=None):
        basedir = basedir or basepath(paths)
        filelist, ignored = self.scanpaths(paths, basedir)
        logging.info("Dispatching files...")
        parts = self.packer.dispatch(filelist)
        for p in parts:
            p.sortfile(self.sortfile)
        with open(self.indexfile, 'w', encoding='utf-8') as f:
            for ln in self.genindex(filelist, paths, ignored, parts):
                f.write(ln + '\n')
        return parts

    def scanpaths(self, paths, prefix=None):
        prefix = prefix or os.path.join(*os.path.commonprefix(tuple(map(splitpath, map(os.path.abspath, paths)))))
        fl = []
        ignored = []
        logging.info("Scanning files...")
        for path in paths:
            if os.path.isfile(path):
                try:
                    if self.ffilter(path):
                        fl.append((os.path.relpath(path, prefix), os.path.getsize(path), os.path.getsize(path)))
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
                                fl.append((os.path.relpath(fn, prefix), os.path.getsize(fn), os.path.getsize(fn)))
                            else:
                                ignored.append(fn)
                        except Exception as ex:
                            logging.exception("Can't access " + fn)
        # estimate compressd size
        if callable(self.compressfunc):
            logging.info("Calculating estimated compressed size...")
            eta = ETA(len(fl), min_ms_between_updates=500)
            for k, v in enumerate(fl):
                filename, size, size2 = v
                fn = os.path.join(prefix, filename)
                try:
                    fl[k] = (filename, size, self.estcompresssize(fn, size))
                except Exception as ex:
                    logging.exception("Can't access " + path)
                eta.print_status()
            eta.done()
        return fl, ignored

    def genindex(self, filelist, paths, ignored, partitions, showignored=True):
        for p in paths:
            yield '# %s' % p
        yield '# %s Total %s files, %s, %s partitions, %s ignored.' % (time.strftime('%Y-%m-%d %H:%M:%S'), len(filelist), sizeof_fmt(sum(map(_ig1, filelist))), len(partitions), len(ignored))
        for pn, part in enumerate(partitions):
            for fn, size, estsize in part.filelist:
                yield "%03d\t%s" % (pn, fn)
        if showignored:
            yield "# Ignored files:"
            for fn in ignored:
                yield "#\t" + fn

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

    def __init__(self, exclude=(), include=()):
        self.exclude = exclude
        self.include = include or ('*',)

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

    def __init__(self, exclude=(), include=()):
        self.exclude = tuple(re.compile(r) for r in exclude)
        self.include = tuple(re.compile(r) for r in include or ('',))

    def __call__(self, filename):
        return (
            any(pat.match(filename) for pat in self.include) and not
            any(pat.match(filename) for pat in self.exclude))

class SizeFilter(Filter):
    '''
    Select files whose size is between `minsize` and `maxsize`.
    If None, it selects all.
    '''

    def __init__(self, maxsize=None, minsize=None):
        self.maxsize = maxsize
        self.minsize = minsize

    def __call__(self, filename):
        filesize = os.path.getsize(filename)
        return ((self.maxsize is None or filesize <= self.maxsize)
            and (self.minsize is None or filesize >= self.minsize))

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

def sortbyext(val):
    head, tail = os.path.split(val[0])
    base, ext = os.path.splitext(tail)
    ext = ext.lower().lstrip('.')
    return exts_ord.get(ext, 999), ext, base, head

def sortbyextlocal(val):
    head, tail = os.path.split(val[0])
    base, ext = os.path.splitext(tail)
    ext = ext.lower().lstrip('.')
    return head, exts_ord.get(ext, 999), ext, base

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

    def addfile(self, filename, origsize, size):
        self.filelist.append((filename, origsize, size))
        self.size += size

    def sortfile(self, level=0):
        '''
        Sort file according to filename:
        0: No sort
        1: Normal sort
        2: Local 7z-style sort (within a directory)
        3: Global 7z-style sort (within a partition)
        '''
        if level == 0:
            return
        if level == 1:
            key = None
        elif level == 2:
            key = sortbyext
        elif level == 3:
            key = sortbyextlocal
        self.filelist.sort(key=key)

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
        for filename, origsize, size in filelist:
            part.addfile(filename, origsize, size)
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
        return partitions

    def single_dispatch(self, filelist, maxsize=0, maxentries=0):
        if maxsize:
            # when maxsize is used, create a default partition (Partition 0)
            #   that will hold files that does not match criteria
            partitions = [Partition(), Partition()]
            pn = startp = 1
        else:
            partitions = [Partition()]
            pn = startp = 0
        for filename, origsize, size in filelist:
            if 0 < maxsize < size:
                partitions[0].addfile(filename, origsize, size)
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
                        partitions[pn].addfile(filename, origsize, size)
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
        filelist.sort(key=_ig1, reverse=True)
        emptyfiles = []
        # dispatch files
        for filename, origsize, size in filelist:
            if size > 0:
                # find most approriate partition
                part = min(partitions, key=_psize)
                # assign it and load the partition with file size
                part.addfile(filename, origsize, size)
            else:
                emptyfiles.append((filename, origsize, size))
        # re-dispatch empty files
        fpp, rem = divmod(len(emptyfiles), len(partitions))
        n = 0
        for part in partitions:
            for i in range(fpp):
                part.addfile(*emptyfiles[n])
                n += 1
        for i in range(rem):
            part.addfile(*emptyfiles[n])
            n += 1
        assert n == len(emptyfiles)
        return partitions

class OutputBase:
    def __init__(self, srcbase, dst, name=None):
        self.srcbase = srcbase
        self.dst = dst
        self.name = name or '%03d'

    def output(self, partitions):
        pass

class OutputCopy(OutputBase):
    def output(self, partitions):
        for pn, part in enumerate(partitions):
            d = os.path.abspath(os.path.join(self.dst, self.name % pn))
            logging.info('Copying to %s' % d)
            eta = ETA(part.size, min_ms_between_updates=500)
            for fn, size, estsize in part.filelist:
                dst = os.path.join(d, fn)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(os.path.join(self.srcbase, fn), dst)
                eta.print_status(estsize)
            eta.done()

class OutputLink(OutputBase):
    def output(self, partitions):
        logging.info('Linking...')
        for pn, part in enumerate(partitions):
            d = os.path.abspath(os.path.join(self.dst, self.name % pn))
            for fn, size, estsize in part.filelist:
                dst = os.path.join(d, fn)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                os.link(os.path.join(self.srcbase, fn), dst)

class Output7z(OutputBase):
    def __init__(self, srcbase, dst, name=None, maxsize=None, extargs=None, cmd7z='7za'):
        self.srcbase = srcbase
        self.dst = dst
        self.name = name or '%03d.7z'
        self.maxsize = maxsize
        self.extargs = extargs or []
        self.cmd7z = cmd7z

    def output(self, partitions):
        parabase = [self.cmd7z, 'a', '-t7z'] + self.extargs
        for pn, part in enumerate(partitions):
            d = os.path.abspath(os.path.join(self.dst, self.name % pn))
            fd, tmpname = tempfile.mkstemp()
            if self.maxsize and part.size > self.maxsize:
                para1 = ['-v' + str(self.maxsize), '--', d, '@' + tmpname]
                cfiles = ['%s.%03d' % (d, i) for i in range(1, int(part.size/self.maxsize)+2)]
            else:
                para1 = ['--', d, '@' + tmpname]
                cfiles = [d]
            print(cfiles)
            if os.path.isfile(cfiles[0]):
                logging.warning('Archive already exists: ' + d)
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    for fn, size, estsize in part.filelist:
                        f.write(fn + '\n')
                logging.info('Creating archive %s...' % (self.name % pn))
                proc = subprocess.Popen(parabase + para1, stdout=sys.stdout, stderr=sys.stderr, cwd=self.srcbase)
                proc.wait()
            except KeyboardInterrupt:
                proc.terminate()
                for fn in cfiles:
                    try:
                        os.remove(fn)
                    except FileNotFoundError:
                        pass
                raise
            finally:
                os.remove(tmpname)

class OutputTar(OutputBase):
    def __init__(self, srcbase, dst, name=None, compression=None):
        self.srcbase = srcbase
        self.dst = dst
        self.ext = 'tar.' + compression if compression else 'tar'
        self.name = name or '%03d.' + self.ext
        self.mode = 'w:' + compression if compression else 'w'

    def output(self, partitions):
        for pn, part in enumerate(partitions):
            d = os.path.abspath(os.path.join(self.dst, self.name % pn))
            if os.path.isfile(d):
                logging.warning('Archive already exists, overwriting: ' + d)
            logging.info('Creating archive %s...' % (self.name % pn))
            with tarfile.open(d, self.mode) as tar:
                for fn, size, estsize in part.filelist:
                    tar.add(os.path.join(self.srcbase, fn), fn)

class OutputZip(OutputBase):
    def __init__(self, srcbase, dst, name=None):
        self.srcbase = srcbase
        self.dst = dst
        self.name = name or '%03d.zip'

    def output(self, partitions):
        for pn, part in enumerate(partitions):
            d = os.path.abspath(os.path.join(self.dst, self.name % pn))
            if os.path.isfile(d):
                logging.warning('Archive already exists, overwriting: ' + d)
            logging.info('Creating archive %s...' % (self.name % pn))
            with zipfile.ZipFile(d, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
                for fn, size, estsize in part.filelist:
                    zipf.write(os.path.join(self.srcbase, fn), fn)


def main():
    parser = argparse.ArgumentParser(description="A flexible file packer with filtering and independent partitioning support.")

    group1 = parser.add_argument_group('Output', 'output control')
    group1.add_argument("-o", "--output", help="output location", default=".")
    group1.add_argument("-i", "--index", help="index file", default="index.txt")
    group1.add_argument("-n", "--name", help="output file/folder name format (Default: %%03d[.ext])")
    group1.add_argument("-f", "--format", help="output format, can be one of 'none', 'copy', 'link', '7z', 'zip', 'tar', 'tar.gz', 'tar.bz2', 'tar.xz' (Default: 7z)", default="7z")
    group1.add_argument("--p7z-args", help="extra arguments for 7z (only for -f 7z) (TIP: use --p7z-args='-xxx' to avoid confusing the argument parser)")
    group1.add_argument("--p7z-cmd", help="7z program to use (Default: 7za, only for -f 7z)", default='7za')
    group1.add_argument("--tar-sort", help="sort file in a partition (only for -f tar.*z). 0: no sort, 1: normal sort, 2(default): 7z-style sort within a directory, 3: 7z-style sort within a partition.", type=int, choices=(0, 1, 2, 3), default=2)

    group2 = parser.add_argument_group('Filter', 'options for filtering files')
    group2.add_argument("--maxfilesize", help="max size of each file")
    group2.add_argument("-m", "--minfilesize", help="min size of each file")
    group2.add_argument("--exclude", help="exclude files that match the glob pattern", action='append')
    group2.add_argument("--include", help="include files that match the glob pattern", action='append')
    group2.add_argument("--exclude-re", help="exclude files that match the regex pattern", action='append')
    group2.add_argument("--include-re", help="include files that match the regex pattern", action='append')
    group2.add_argument("-a", "--after", help="select files whose modification time is after this value (Format: %%Y%%m%%d%%H%%M%%S, eg. 20140101120000, use local time zone)")
    group2.add_argument("-b", "--before", help="select files whose modification time is before this value (Format: %%Y%%m%%d%%H%%M%%S, eg. 20150601000000, use local time zone)")

    group3 = parser.add_argument_group('Partition', 'partition methods')
    group3.add_argument("-s", "--maxpartsize", help="max partition size", default=0)
    group3.add_argument("--maxfilenum", help="max file number per partition", type=int, default=0)
    group3.add_argument("-p", "--part", help="partition number (overrides: -s, --maxfilenum)", type=int)

    parser.add_argument("PATH", nargs='+', help="Paths to archive")
    args = parser.parse_args()

    pathlist = args.PATH
    basedir = basepath(pathlist)

    if args.part:
        packer = PartNumberLimitPacker(args.part)
    elif args.maxpartsize or args.maxfilenum:
        packer = LimitPacker(human2bytes(args.maxpartsize), args.maxfilenum)
    else:
        packer = SingleVolumePacker()

    ffilter = TrueFilter()
    if args.maxfilesize or args.minfilesize:
        ffilter |= SizeFilter(human2bytes(args.maxfilesize), human2bytes(args.minfilesize))
    if args.exclude or args.include:
        ffilter |= GlobFilter(args.exclude, args.include)
    if args.exclude_re or args.include_re:
        ffilter |= RegexFilter(args.exclude_re, args.include_re)
    if args.after or args.before:
        after = time.mktime(time.strptime(args.after, '%Y%m%d%H%M%S')) if args.after else None
        before = time.mktime(time.strptime(args.before, '%Y%m%d%H%M%S')) if args.before else None
        ffilter |= TimeFilter(after, before, 'm')

    compressfunc = None
    sortfile = 0
    if args.format == 'none':
        output = OutputBase(basedir, args.output, args.name)
    elif args.format == 'copy':
        output = OutputCopy(basedir, args.output, args.name)
    elif args.format == 'link':
        output = OutputLink(basedir, args.output, args.name)
    elif args.format == '7z':
        compressfunc = lzma.compress
        output = Output7z(basedir, args.output, args.name, human2bytes(args.maxpartsize), shlex.split(args.p7z_args), args.p7z_cmd)
    elif args.format == 'zip':
        compressfunc = zlib.compress
        output = OutputZip(basedir, args.output, args.name)
    elif args.format.startswith('tar'):
        ext = args.format.split('.')
        compression = ext[1] if len(ext) == 2 else None
        if compression == 'gz':
            compressfunc = zlib.compress
        elif compression == 'bz2':
            compressfunc = bz2.compress
        elif compression == 'xz':
            compressfunc = lzma.compress
        elif compression is None:
            pass
        else:
            raise ValueError('unsupported compression method ' + compression)
        sortfile = args.tar_sort
        output = OutputTar(basedir, args.output, args.name, compression)
    else:
        raise ValueError('unsupported output format ' + args.format)

    vol = Volume(packer, ffilter, os.path.join(args.output, args.index), output, compressfunc, sortfile)
    vol.run(pathlist, basedir)

if __name__ == '__main__':
    main()
