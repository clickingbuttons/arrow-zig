#!/bin/env python
# Requires pyarrow.
# `zig build` -> run this
from os import path
import sys
from ctypes import *
import pyarrow as pa
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

rb = pa.RecordBatch._import_from_c(addressof(arr), addressof(schema))
tb = pa.Table.from_batches([rb])
tb.validate(full=True)

expected = {
	"a": [None, 32, 33, 34],
	"b": [None, [1, 2, 3], [4, 5, 6], [7, 8, 9]],
	"c": [None, b'hello', b'friend', b'goodbye'],
	"d": [None, [1, 2, 3], [4, 5, 6], [7, 8, 9]],
	"e": [None, [1, 2, 3], [4, 5, 6], [7, 8, 9]],
	"f": [None, {'a': 1, 'b': 1}, {'a': 2, 'b': 4}, {'a': None, 'b': 9}],
	"g": [None, 1.0, 3.0, 5],
	"h": [None, 1.0, 3.0, 5],
	"i": [None, b'hello', b'there', b'friend'],
	"j": [None, [(b'hello', 1)], [(b'arrow', 2), (b'map', None)], [(b'goodbye', 3)]],
}

code = 0
for k in expected.keys():
	actual = [v.as_py() for v in tb.column(k)]
	if expected[k] != actual:
		code = 1
		print("column", k, "expected", expected[k], "got", actual)

reader = tb.to_reader()
with pa.OSFile("sample.arrow", "wb") as sink:
	with pa.ipc.new_file(sink, schema=reader.schema) as writer:
		for batch in reader:
			writer.write(batch)

sys.exit(code)
