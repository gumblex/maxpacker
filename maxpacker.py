#!/usr/bin/env python3
import sys
import os
import time
import argparse
import tempfile
from subprocess import Popen, PIPE

import zlib
# 7z LZMA, Python 3.3+
import lzma
from eta import ETA
from pprint import pprint
import math
from collections import Counter

DEFLATE = zlib.compress
LZMA = lzma.compress

def entropy(s):
	p, lns = Counter(s), len(s)
	return -sum(count/lns * math.log(count/lns, 2) for count in p.values())/8

class dllist:
	def __init__():
		root = []
		root[:] = [root, root, None]
		return root

	def __iter__(root):
		# start at the first node
		curr = root[1]
		while curr is not root:
			# yield the curr[KEY]
			yield curr[2]
			# move to next node
			curr = curr[1]

	def append(root, value):
		last = root[0]
		last[1] = root[0] = [last, root, value]

	def remove(node):
		prev_link, next_link, _ = node
		prev_link[1] = next_link
		next_link[0] = prev_link

class Partition:
	def __init__(self):
		self.filelist = []
		self.size = 0
		self.numfiles = 0
		self.dumped = False

	def __repr__(self):
		return "<Partition size=%r numfiles=%r>" % (self.size, self.numfiles)
	
	def __len__(self):
		return self.numfiles

	def __iter__(self):
		return iter(self.filelist)
	
	def __getitem__(self, key):
		return self.filelist[key]
	
	def __bool__(self):
		return self.numfiles != 0

	def append(self, value):
		self.filelist.append(value)

	def pop(self):
		return self.filelist.pop(value)

class Volumes:
	"""Manages Volumes containing several Partitions"""
	def __init__(self, prefix, filelist, maxsize, maxentries=0, samplesize=1024, algo=DEFLATE):
		# file = [path, size, partition]
		self.prefix = prefix
		self.rawfilelist = filelist
		self.maxsize = maxsize
		self.maxentries = maxentries
		self.samplesize = samplesize
		self.algo = algo
		self.filelist = []
	
	def init(self):
		eta = ETA(len(self.rawfilelist)-1, min_ms_between_updates=500)
		for f, s in self.rawfilelist:
			eta.print_status()
			self.filelist.append([f, self.estimatesize(f, s), s, 0])
		eta.done()
		self.minfilesize = min(map(lambda x: x[1], self.filelist))
		self.partitions = self.dispatchfiles(self.filelist, self.maxsize, self.maxentries)
		multipart = 1
		while self.partitions[0]:
			multipart += 1
			temppart = self.dispatchfiles(self.partitions[0].filelist + self.partitions.pop().filelist, self.maxsize*multipart, self.maxentries)
			self.partitions[0] = temppart[0]
			self.partitions.extend(filter(None, temppart[1:]))
		self.partitions.pop(0)
	
	def dispatchfiles(self, filelist, maxsize, maxentries):
		partitions = []
		# number of partitions created, our return value
		num_parts_created = 0

		# when maxsize is used, create a default partition (partition 0)
		#   that will hold files that does not match criteria
		if maxsize > 0:
			partitions.append(Partition())
			part_head = partitions[0]
			num_parts_created += 1
			default_partition = part_head

		# create a first data partition and keep a pointer to it
		partitions.append(Partition())
		part_head = partitions[-1]
		num_parts_created += 1
		start_partition = part_head
		start_partition_index = num_parts_created - 1

		# for each file, associate it with current partition
		#   (or default_partition)
		current_partition_index = start_partition_index
		for head in filelist:
			# maxsize provided and file size > maxsize,
			#   associate file to default partition
			if (maxsize > 0) and (head[1] > maxsize):
				head[3] = 0
				partitions[0].append(head)
				default_partition.size += head[1]
				default_partition.numfiles += 1
			else:
				# examine each partition
				while current_partition_index < len(partitions):
					# if file does not fit in partition
					if (maxentries > 0) and (part_head.numfiles + 1 > maxentries) or ((maxsize > 0) and (part_head.size + head[1] > maxsize)):
						# and we reached last partition, chain a new one
						if current_partition_index+1 == len(partitions):
							partitions.append(Partition())
							part_head = partitions[-1]
							num_parts_created += 1
						else:
							# examine next partition
							part_head = partitions[current_partition_index+1]
						current_partition_index += 1
					else:
						# file fits in current partition, add it
						head[3] = current_partition_index
						part_head.append(head)
						part_head.size += head[1]
						part_head.numfiles += 1
						# examine next file
						break

			# examine next file
			# come back to the first partition
			current_partition_index = start_partition_index
			part_head = start_partition
		return partitions

	def estimatesize(self, filename, fsize, err=0.1):
		if not fsize:
			return 0
		if fsize <= self.samplesize:
			with open(filename, 'rb') as f:
				sample = f.read()
		else:
			with open(filename, 'rb') as f:
				f.seek(int((fsize - self.samplesize)/2))
				sample = f.read(self.samplesize)
		compsize = len(self.algo(sample)) / len(sample)
		if compsize > entropy(sample):
			return int(fsize * compsize)
		else:
			return int(fsize * compsize / (1+err))

def dumppartition7z(vol, cmd, cwd, run):
	fd, tmpname = tempfile.mkstemp()
	os.close(fd)
	tempcmd = ['7za', 'a', '-t7z']
	tempcmd.append("-i@" + tmpname)
	tempcmd.extend(cmd)
	try:
		with open(vol.prefix + ".index.txt", 'w') as index:
			for k,p in enumerate(vol.partitions):
				filename = os.path.abspath(vol.prefix + str(k).zfill(3) + ".7z")
				if p.size > vol.maxsize:
					addpara = ["-v" + str(vol.maxsize), '--', filename]
				else:
					addpara = ['--', filename]
				with open(tmpname, 'w') as f:
					for fn in p.filelist:
						f.write(os.path.relpath(fn[0], cwd) + '\n')
						index.write('\t'.join((os.path.basename(filename), os.path.relpath(fn[0], cwd), str(os.path.getsize(fn[0])))) + '\n')
				if run:
					print(p.size)
					proc = Popen(tempcmd + addpara, stdout=sys.stdout, stderr=sys.stderr, cwd=cwd).wait()
	finally:
		os.remove(tmpname)

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
		#assert num.isdigit() and letter in symbols
		num = float(num)
		prefix = {symbols[0]:1}
		for i, s in enumerate(symbols[1:]):
			prefix[s] = 1 << (i+1)*10
		return int(num * prefix[letter])

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

def findparent(paths):
	parent = splitpath(paths[0])
	for p in paths:
		pl = splitpath(p)
		for k,i in enumerate(pl):
			if k >= len(parent):
				break
			elif parent[k] != i:
				break
		parent = parent[:k]
	return os.path.join(*parent)

def main():
	parser = argparse.ArgumentParser(description="Independent compressed volumes maxium packer.")
	parser.add_argument("-s", "--maxsize", help="max volume size", default="1g")
	parser.add_argument("-n", "--dry-run", action="store_true", help="only allocate files")
	parser.add_argument("-o", "--prefix", help="output files prefix")
	# group = parser.add_mutually_exclusive_group()
	# reserved for tar, zip, etc.
	parser.add_argument("FOLDER", help="Folder to archive")
	args, unknown = parser.parse_known_args()
	print("Scanning files...")
	folderlist = [args.FOLDER]
	cmd = []
	for p in unknown:
		if os.path.exists(p):
			folderlist.append(p)
		else:
			cmd.append(p)
	parentdir = findparent(folderlist)
	fl = []
	for folder in folderlist:
		if os.path.isfile(folder):
			fl.append((folder, os.path.getsize(folder)))
		else:
			for root, subFolders, files in os.walk(folder):
				for name in files:
					fn = os.path.join(root, name)
					fl.append((fn, os.path.getsize(fn)))

	print("Calculating estimated compressed size...")
	vol = Volumes(os.path.basename(parentdir) if args.prefix is None else args.prefix, fl, human2bytes(args.maxsize), algo=LZMA)
	try:
		vol.init()
		print("Compressing...")
		dumppartition7z(vol, cmd, parentdir, not args.dry_run)
		print("Done.")
	except KeyboardInterrupt:
		pass

if __name__ == '__main__':
	main()
