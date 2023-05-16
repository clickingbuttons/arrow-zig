#!/bin/env python
# Requires pyarrow.
# `zig build` -> python integration_test.py
from os import path
import sys
from ctypes import *
import pyarrow
from sys import platform

def soExt():
	if platform == "win32":
		return "dll"
	if platform == "darwin":
		return "dylib"
	return "so"

libname = path.abspath(path.join("zig-out", "lib", "libarrow-zig." + soExt()))
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
res = lib.sampleRecordBatch(byref(arr), byref(schema))
if res != 0:
		raise Exception(res)

rb = pyarrow.RecordBatch._import_from_c(addressof(arr), addressof(schema))
tb = pyarrow.Table.from_batches([rb])
tb.validate(full=True)

expected = {
	"a": [None, "hello", "goodbye"],
	"b": [None, 1, 2],
	"c": [None, [3,4], [5,6]],
	"d": [None, [3,4], [5,6]],
	"e": [None, { "a": 3, "b": 6}, { "a": 4, "b": 7 }],
	"f": [None, 3, 4],
	"g": [None, 3, 1],
	"h": [None, 3, 4],
}

code = 0
for k in expected.keys():
	actual = [v.as_py() for v in tb.column(k)]
	if expected[k] != actual:
		code = 1
		print("column", k, "expected", expected[k], "got", actual)

sys.exit(code)
