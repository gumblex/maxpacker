import sys
import os
import time
import zlib
import lzma
import math
from collections import Counter

files = [
["a",34,0],
["b",13,0],
["c",65,0],
["d",58,0],
["e",89,0],
["f",12,0],
["g",30,0],
["h",310,0],
]

def entropy(s):
	p, lns = Counter(s), float(len(s))
	return -sum(count/lns * math.log(count/lns, 2) for count in p.values())/8

def estimatesize(filename, samplesize, err=0):
	fsize = os.path.getsize(filename)
	if not fsize:
		return 0
	if fsize <= samplesize:
		with open(filename, 'rb') as f:
			sample = f.read()
	else:
		with open(filename, 'rb') as f:
			f.seek(int((fsize - samplesize)/2))
			sample = f.read(samplesize)
	return int(fsize * len(zlib.compress(sample)) / len(sample) / (1+err))

samplesize = 1024
testres = []

try:
	for root, subFolders, files in os.walk(sys.argv[1]):
		for name in files:
			try:
				filename = os.path.join(root,name)
				print(filename)
				with open(filename, 'rb') as f:
					fbyte = f.read()
				if not fbyte:
					continue
				size = len(lzma.compress(fbyte))
				sttime = time.perf_counter()
				estsize = estimatesize(filename, samplesize)
				testres.append(((estsize-size)/size, time.perf_counter()-sttime))
			except PermissionError:
				pass
except KeyboardInterrupt:
	pass

print(samplesize, sum(map(lambda x: x[0], testres))/len(testres), sum(map(lambda x: x[1], testres))/len(testres))
