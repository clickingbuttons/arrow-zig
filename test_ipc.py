#!/bin/env python
# Requires pyarrow.

import sys
import pyarrow as pa

tb = pa.ipc.open_file("testdata/sample_written.arrow").read_all()

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

sys.exit(code)
