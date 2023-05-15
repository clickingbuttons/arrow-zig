#!/bin/env python
# Requires pyarrow.
# `zig build` -> python integration_test.py
import os
from ctypes import *
import pyarrow

libname = os.path.abspath(os.path.join(os.path.dirname(__file__), "zig-out", "lib", "libarrow-zig.so"))
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

class QueryOptions(Structure):
	_fields_ = [
		('host', POINTER(c_char)),
		('port', c_uint32),
		('default_database', POINTER(c_char)),
		('user', POINTER(c_char)),
		('password', POINTER(c_char)),
		('compression', c_int),
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

