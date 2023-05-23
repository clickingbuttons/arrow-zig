#!/bin/env python
import sys
import pyarrow as pa

def read_file(fname):
	with pa.OSFile(fname, "r") as f:
		with pa.ipc.open_file(f) as reader:
			return reader.read_all()

tb = read_file(sys.argv[1])
print(tb)
