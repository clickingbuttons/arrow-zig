#!/bin/env python
# Requires pyarrow.
# `zig build` -> python integration_test.py
from os import path
from ctypes import *
import pyarrow
from sys import platform

def soExt():
	if platform == "win32":
		return "dll"
	if platform == "darwin":
		return "dylib"
	return "so"

libname = path.abspath(path.join(path.dirname(__file__), "zig-out", "lib", "libarrow-zig." + soExt()))
lib = CDLL(libname)

class ArrowArray(Structure):
	pass
ArrowArray._fields_ = [
	('length', c_int64),
	('null_count', c_int64), 
	('offset', c_int64), 
	('n_buffers', c_int64), 
	('n_children', c_int64), 
	('buffers', POINTER(c_void_p)),
	('children', POINTER(POINTER(ArrowArray))),
	('dictionary', POINTER(ArrowArray)),
	('release', CFUNCTYPE(None, POINTER(ArrowArray))),
	('private_data', c_void_p),
]

class ArrowSchema(Structure):
	pass
ArrowSchema._fields_ = [
	('format', POINTER(c_char)),
	('name', POINTER(c_char)),
	('metadata', POINTER(c_char)),
	('flags', c_int64),
	('n_children', c_int64),
	('children', POINTER(POINTER(ArrowSchema))),
	('dictionary', POINTER(ArrowSchema)),
	('release', CFUNCTYPE(None, POINTER(ArrowSchema))),
	('private_data', c_void_p),
]

arr = ArrowArray()
schema = ArrowSchema()
res = lib.testArray(byref(arr), byref(schema))
if res != 0:
		raise Exception(res)

# print(schema.children[0].contents.name[:2])

rb = pyarrow.RecordBatch._import_from_c(addressof(arr), addressof(schema))
tb = pyarrow.Table.from_batches([rb])
print(tb)

