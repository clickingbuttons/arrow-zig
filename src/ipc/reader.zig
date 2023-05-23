const std = @import("std");
const Footer = @import("./gen/Footer.fb.zig").Footer;
const Message = @import("./gen/Message.fb.zig").Message;
const schema_mod = @import("./gen/Schema.fb.zig");
const record_batch = @import("./gen/RecordBatch.fb.zig");
const dictionary_batch = @import("./gen/DictionaryBatch.fb.zig");
const DictionaryEncoding = @import("./gen/DictionaryEncoding.fb.zig").DictionaryEncodingT;
const array_mod = @import("../array/array.zig");
const field_mod = @import("./gen/Field.fb.zig");
const FieldNode = @import("./gen/FieldNode.fb.zig").FieldNodeT;
const FieldType = @import("./gen/Type.fb.zig").TypeT;
const tags = @import("../tags.zig");
const abi = @import("../abi.zig");

const Array = array_mod.Array;
const BufferAlignment = array_mod.BufferAlignment;
const BufferT = []align(BufferAlignment) u8;
// We always have to read the entire schema, record batch, and dictionary headers from flatbuffers.
// For this reason we unpack to these nice types to avoid writing `.?` on non-int property accesses.
const Schema = schema_mod.SchemaT;
const RecordBatch = record_batch.RecordBatchT;
const DictionaryBatch = dictionary_batch.DictionaryBatchT;
const Field = field_mod.FieldT;
const magic = "ARROW1";
const MessageLen = i32;
const continuation = @bitCast(MessageLen, @as(u32, 0xffffffff));

const log = std.log.scoped(.arrow_ipc);

pub const IpcError = error {
	InvalidMagicLen,
	InvalidMagic,
	InvalidLen,
	InvalidContinuation,
	InvalidMessageType,
	InvalidSchemaMesssage,
	MissingSchemaMessage,
	InvalidRecordBatch,
	InvalidRecordBatchHeader,
	InvalidBitWidth,
	InvalidFieldTag,
	InvalidDictionaryBatchHeader,
	InvalidDictionaryIndexType,
	DictNotFound,
	NoDictionaryData,
	InvalidNumDictionaryBuffers,
};

fn toDictTag(dict: *DictionaryEncoding) !tags.Tag {
	return .{
		.Dictionary = .{
			.index = if (dict.indexType) |t| switch (t.bitWidth) {
				8 => .i8,
				16 => .i16,
				32 => .i32,
				else => |w| {
					log.warn("dictionary {d} has invalid index bit width {d}", .{ dict.id, w });
					return IpcError.InvalidDictionaryIndexType;
				}
			} else {
				log.warn("dictionary {d} missing index type", .{ dict.id });
				return IpcError.InvalidDictionaryIndexType;
			}
		}
	};
}

fn toFieldTag(field: Field) !tags.Tag {
	return switch(field.type) {
		// .NONE: void,
		.Null => .Null,
		.Int => |maybe_i| {
			if (maybe_i) |i| {
				return .{
					.Int = .{
						.nullable = field.nullable,
						.signed = i.is_signed,
						.bit_width = switch (i.bitWidth) {
							8 => ._8,
							16 => ._16,
							32 => ._32,
							64 => ._64,
							else => |w| {
								log.warn("int field {s} invalid bit width {d}", .{ field.name, w });
								return IpcError.InvalidBitWidth;
							}
						}
					}
				};
			}
			log.warn("int field {s} missing bit width", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.FloatingPoint => |maybe_f| {
			if (maybe_f) |f| {
				return .{
					.Float = .{
						.nullable = field.nullable,
						.bit_width = switch (f.precision) {
							.HALF => ._16,
							.SINGLE => ._32,
							.DOUBLE => ._64,
						}
					}
				};
			}
			log.warn("float field {s} missing precision", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.Binary => .{ .Binary = .{ .large = false, .utf8 = false } },
		.LargeBinary => .{ .Binary = .{ .large = true, .utf8 = false } },
		.Utf8 => .{ .Binary = .{ .large = false, .utf8 = true } },
		.LargeUtf8 => .{ .Binary = .{ .large = true, .utf8 = true } },
		.Bool => .{ .Bool = .{ .nullable = field.nullable } },
		// .Decimal: ?*DecimalT,
		.Date => |maybe_d| {
			if (maybe_d) |d| {
				return .{
					.Date = .{
						.nullable = field.nullable,
						.unit = switch (d.unit) {
							.DAY => .day,
							.MILLISECOND => .millisecond,
						}
					}
				};
			}
			log.warn("date field {s} missing unit", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.Time => |maybe_t| {
			if (maybe_t) |t| {
				return .{
					.Time = .{
						.nullable = field.nullable,
						.unit = switch (t.unit) {
							.SECOND => .second,
							.MILLISECOND => .millisecond,
							.MICROSECOND => .microsecond,
							.NANOSECOND => .nanosecond,
						}
					}
				};
			}
			log.warn("time field {s} missing unit", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.Timestamp => |maybe_ts| {
			if (maybe_ts) |ts| {
				return .{
					.Timestamp = .{
						.nullable = field.nullable,
						.unit = switch (ts.unit) {
							.SECOND => .second,
							.MILLISECOND => .millisecond,
							.MICROSECOND => .microsecond,
							.NANOSECOND => .nanosecond,
						},
						.timezone = ts.timezone,
					}
				};
			}
			log.warn("timestamp field {s} missing unit", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.Duration => |maybe_d| {
			if (maybe_d) |d| {
				return .{
					.Duration = .{
						.nullable = field.nullable,
						.unit = switch (d.unit) {
							.SECOND => .second,
							.MILLISECOND => .millisecond,
							.MICROSECOND => .microsecond,
							.NANOSECOND => .nanosecond,
						},
					}
				};
			}
			log.warn("duration field {s} missing unit", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.Interval => |maybe_i| {
			if (maybe_i) |i| {
				return .{
					.Interval = .{
						.nullable = field.nullable,
						.unit = switch (i.unit) {
							.YEAR_MONTH => .year_month,
							.DAY_TIME => .day_time,
							.MONTH_DAY_NANO => .month_day_nanosecond,
						},
					}
				};
			}
			log.warn("timestamp field {s} missing unit", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.List => .{ .List = .{ .nullable = field.nullable, .large = false } },
		.LargeList => .{ .List = .{ .nullable = field.nullable, .large = true } },
		.Struct_ => .{ .Struct = .{ .nullable = field.nullable } },
		.Union => |maybe_u| {
			if (maybe_u) |u| {
				return .{
					.Union = .{
						.nullable = field.nullable,
						.dense = switch (u.mode) {
							.Dense => true,
							.Sparse => false,
						}
					}
				};
			}
			log.warn("union field {s} missing mode", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.FixedSizeBinary => |maybe_b| {
			if (maybe_b) |b| {
				return .{
					.FixedBinary = .{
						.nullable = field.nullable,
						.fixed_len = b.byteWidth,
					}
				};
			}
			log.warn("fixed size binary field {s} missing byte width", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.FixedSizeList => |maybe_f| {
			if (maybe_f) |f| {
				return .{
					.FixedList = .{
						.nullable = field.nullable,
						.fixed_len = f.listSize,
						.large = false,
					}
				};
			}
			log.warn("fixed size list field {s} missing fixed length", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		// .Map: ?*MapT,
		// .RunEndEncoded: ?*RunEndEncodedT,
		else => |t| {
			log.warn("field {s} unknown type {any}", .{ field.name, t });
			return IpcError.InvalidFieldTag;
		}
	};
}

fn toTag(field: Field) !tags.Tag {
	if (field.dictionary) |d| return toDictTag(d);

	return toFieldTag(field);
}

fn nFields2(f: Field) usize {
	var res: usize = 0;
	for (f.children.items) |c| {
		res += 1 + nFields2(c);
	}
	return res;
}

fn nFields(schema: Schema) usize {
	var res: usize = 0;
	for (schema.fields.items) |f| {
		res += 1 + nFields2(f);
	}
	return res;
}

fn nBuffers2(f: Field) !usize {
	var res: usize = 0;
	res += (try toTag(f)).abiLayout().nBuffers();
	for (f.children.items) |c| {
		res += try nBuffers2(c);
	}
	return res;
}

fn nBuffers(schema: Schema) !usize {
	var res: usize = 0;
	for (schema.fields.items) |f| {
		res += try nBuffers2(f);
	}
	return res;
}

const RecordBatchReader = struct {
	const Dictionaries = std.AutoHashMap(i64, struct {
		length: usize,
		null_count: usize,
		buf0: std.ArrayList(u8),
		buf1: std.ArrayList(u8),

		pub fn deinit(self: @This()) void {
			self.buf0.deinit();
			self.buf1.deinit();
		}
	});

	allocator: std.mem.Allocator,
	arena: std.heap.ArenaAllocator,
	source: std.io.StreamSource,
	schema: Schema,
	n_fields: usize,
	n_buffers: usize,
	owns_file: bool = false,

	node_index: usize = 0,
	buffer_index: usize = 0,
	dictionaries: Dictionaries,

	const Self = @This();

	pub fn init(allocator: std.mem.Allocator, source: std.io.StreamSource) !Self {
		var res = Self {
			.allocator = allocator,
			.arena = std.heap.ArenaAllocator.init(allocator),
			.source = source,
			.schema = undefined,
			.n_fields = undefined,
			.n_buffers = undefined,
			.dictionaries = Dictionaries.init(allocator),
		};
		res.schema = try res.readSchema();
		res.n_fields = nFields(res.schema);
		res.n_buffers = try nBuffers(res.schema);
		return res;
	}

	pub fn initFile(allocator: std.mem.Allocator, file: std.fs.File, take_ownership: bool) !Self {
		var source = std.io.StreamSource{ .file = file };
		try source.seekTo(8);
		var res = try init(allocator, source);
		res.owns_file = take_ownership;
		return res;
	}

	pub fn initFilePath(allocator: std.mem.Allocator, path: []const u8) !Self {
		var file = try std.fs.cwd().openFile(path, .{});
		return initFile(allocator, file, true);
	}

	pub fn deinit(self: *Self) void {
		if (self.owns_file) self.source.file.close();
		var iter = self.dictionaries.valueIterator();
		while (iter.next()) |d| {
			d.deinit();
		}
		self.dictionaries.deinit();
		self.arena.deinit();
	}

	fn readMessageLen(self: *Self) !usize {
		// > This component was introduced in version 0.15.0 in part to address the 8-byte alignment
		// > requirement of Flatbuffers.
		var res = try self.source.reader().readIntLittle(MessageLen);
		if (res == continuation) {
			return self.readMessageLen();
		}

		return @intCast(usize, res);
	}

	fn readMessage(self: *Self) !?Message {
		// <continuation: 0xFFFFFFFF> (optional)
		// <metadata_size: int32>
		// <metadata_flatbuffer: bytes>
		// <padding to 8 byte boundary>
		// <message body>
		const message_len = try self.readMessageLen();
		if (message_len == 0) return null; // EOS

		var message_buf = try self.arena.allocator().alloc(u8, message_len);
		const n_read = try self.source.reader().read(message_buf);
		if (n_read != message_buf.len) {
			return IpcError.InvalidLen;
		}
		// Have not yet experienced need for padding.
		std.debug.assert(n_read % 8 == 0);
		return Message.GetRootAs(message_buf, 0);
	}

	fn readMessageBody(self: *Self, size: i64) ![]u8 {
		const real_size = @intCast(usize, size);
		const res = try self.arena.allocator().alignedAlloc(u8, abi.BufferAlignment, real_size);
		const n_read = try self.source.reader().read(res);
		if (n_read != res.len) {
			log.err("record batch ended early", .{});
			return IpcError.InvalidRecordBatch;
		}

		return res;
	}

	fn readSchema(self: *Self) !Schema {
		if (try self.readMessage()) |message| {
			if (message.HeaderType() != .Schema) {
				log.err("expected initial schema message, got {any}", .{ message.HeaderType() });
				return IpcError.InvalidSchemaMesssage;
			}
			if (message.Header()) |header| {
				const allocator = self.arena.allocator();
				return schema_mod.Schema.init(header.bytes, header.pos).Unpack(.{ .allocator = allocator });
			} else {
				return IpcError.InvalidSchemaMesssage;
			}
		}

		return IpcError.MissingSchemaMessage;
	}

	fn readBuffers(self: *Self, allocator: std.mem.Allocator, batch: RecordBatch, body_len: i64) ![]BufferT {
		var buffers = try allocator.alloc(BufferT, batch.buffers.items.len);
		errdefer allocator.free(buffers);
		for (batch.buffers.items, 0..) |info, i| {
			buffers[i] = try allocator.alignedAlloc(u8, BufferAlignment, @intCast(usize, info.length));
			const n_read = try self.source.read(buffers[i]);
			if (buffers[i].len != n_read) {
				log.err("expected {d} bytes in record batch body, got {d}",
					.{ buffers[i].len, n_read });
				return IpcError.InvalidRecordBatch;
			}

			const next_offset = if (i == buffers.len - 1) body_len else batch.buffers.items[i + 1].offset;
			const seek = next_offset - (info.offset + info.length);
			try self.source.seekBy(seek);
		}

		return buffers;
	}

	fn readField(
		self: *Self,
		buffers: []BufferT,
		nodes: []FieldNode,
		field: Field
	) !*Array {
		const allocator = self.allocator;
		const tag = try toTag(field);
		log.debug("read field \"{s}\" type {any} n_children {d}",
			.{ field.name, tag, field.children.items.len });

		var res = try allocator.create(Array);
		res.* = .{
			.tag = tag,
			.name = field.name,
			.allocator = allocator,
			.length = @intCast(usize, nodes[self.node_index].length),
			.null_count = @intCast(usize, nodes[self.node_index].null_count),
			.bufs = .{ &.{}, &.{}, &.{} },
			.children = try allocator.alloc(*Array, field.children.items.len),
		};
		self.node_index += 1;

		for (0..res.tag.abiLayout().nBuffers()) |i| {
			res.bufs[i] = buffers[self.buffer_index];
			self.buffer_index += 1;
		}

		for (field.children.items, 0..) |field_c, i| {
			res.children[i] = try self.readField(buffers, nodes, field_c);
		}
		if (field.dictionary) |d| {
			log.debug("read field \"{s}\" dictionary {d}", .{ field.name, d.id });
			if (self.dictionaries.get(d.id)) |v| {
				// Copy out dictionary. Yes, this sucks for performance. Thank the shitty spec :)
				var dict_values = try allocator.create(Array);
				dict_values.* = .{
					.tag = try toFieldTag(field),
					.name = "dict values",
					.allocator = allocator,
					.length = v.length,
					.null_count = v.null_count,
					.bufs = .{
						try allocator.alignedAlloc(u8, BufferAlignment, v.buf0.items.len),
						try allocator.alignedAlloc(u8, BufferAlignment, v.buf1.items.len),
						&.{}
					},
					.children = &.{},
				};
				@memcpy(dict_values.bufs[0], v.buf0.items);
				@memcpy(dict_values.bufs[1], v.buf1.items);
				res.children = try allocator.alloc(*Array, 1);
				res.children[0] = dict_values;
			} else {
				return IpcError.DictNotFound;
			}
		}
		return res;
	}

	/// Caller owns Array.
	fn readBatch(self: *Self, batch: RecordBatch, body_len: i64) !*Array {
		// https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message
		// > Fields and buffers are flattened by a pre-order depth-first traversal of the fields in the
		// > record batch.
		log.debug("read batch len {d} compression {any}", .{ batch.length, batch.compression });
		const allocator = self.allocator;

		// Quickly check that the number of buffers and field nodes matches the schema.
		if (batch.buffers.items.len != self.n_buffers) {
			log.warn("skipped batch with {d} buffers (schema expects {d})",
				.{ batch.buffers.items.len, self.n_buffers });
			return IpcError.InvalidLen;
		}
		if (batch.nodes.items.len != self.n_fields) {
			log.warn("skipped batch with {d} fields (schema expects {d})",
				.{ batch.nodes.items.len, self.n_fields });
			return IpcError.InvalidLen;
		}

		// Read flattened buffers
		const buffers = try self.readBuffers(allocator, batch, body_len);
		defer allocator.free(buffers);

		// Recursively read tags, name, and buffers into arrays from `schema.fields`
		self.node_index = 0;
		self.buffer_index = 0;
		var children = try allocator.alloc(*Array, self.schema.fields.items.len);
		for (self.schema.fields.items, 0..) |f, i| {
			children[i] = try self.readField(buffers, batch.nodes.items, f);
		}

		const res = try allocator.create(Array);
		res.* = .{
			.tag = .{ .Struct = .{ .nullable = false } },
			.name = "record batch",
			.allocator = allocator,
			.length = @intCast(usize, batch.length),
			.null_count = 0,
			.bufs = .{ &.{}, &.{}, &.{} },
			.children = children,
		};
		return res;
	}

	/// Reader owns dict.
	fn readDict(self: *Self, dict: DictionaryBatch, body_len: i64) !void {
		// We own the dictionaries due to any message being able to update them. The values are copied
		// out into arrays.
		// A maybe better alternative is adding reference counting support to Array. However, that will
		// result in previous record batches referencing that dictionary being mutated which I'd argue
		// is unexpected behavior.
		const allocator = self.arena.allocator();

		log.debug("read_dict {d}", .{dict.id});
		const batch = if (dict.data) |d| d.* else return IpcError.NoDictionaryData;
		const n_expected = comptime abi.Array.Layout.Dictionary.nBuffers();
		std.debug.assert(n_expected == 2);
		const n_actual = batch.buffers.items.len;
		if (n_expected != n_actual) {
			log.warn("expected dictionary data to have {d} buffers, got {d}", .{ n_expected, n_actual });
			return IpcError.InvalidNumDictionaryBuffers;
		}
		const node = batch.nodes.items[0];
		const buffers = try self.readBuffers(allocator, batch, body_len);
		defer allocator.free(buffers);

		if (dict.isDelta) {
			if (self.dictionaries.getPtr(dict.id)) |existing| {
				try existing.buf0.appendSlice(buffers[0]);
				try existing.buf1.appendSlice(buffers[1]);
			} else {
				log.warn("ignoring delta for non-existant dictionary {d}", .{ dict.id });
			}
		} else {
			var buf0 = std.ArrayList(u8).init(allocator);
			var buf1 = std.ArrayList(u8).init(allocator);
			try buf0.appendSlice(buffers[0]);
			try buf1.appendSlice(buffers[1]);

			if (try self.dictionaries.fetchPut(dict.id, .{
					.length = @intCast(usize, node.length),
					.null_count = @intCast(usize, node.null_count),
					.buf0 = buf0,
					.buf1 = buf1,
				})) |existing| {
				log.warn("spec does not support replacing dictionary for dictionary {d}", .{ dict.id });
				existing.value.deinit();
			}
		}
	}

	/// Caller owns Array.
	pub fn next(self: *Self) !?*Array {
		const allocator = self.arena.allocator();
		if (try self.readMessage()) |message| {
			self.node_index = 0;
			self.buffer_index = 0;
			switch (message.HeaderType()) {
				.NONE, .Schema => {
					log.warn("ignoring unexpected message {any}", .{ message.HeaderType() });
					try self.source.seekBy(message.BodyLength());
				},
				.DictionaryBatch => {
					if (message.Header()) |header| {
						const dict = try dictionary_batch.DictionaryBatch
							.init(header.bytes, header.pos)
							.Unpack(.{ .allocator = allocator });
						try self.readDict(dict, message.BodyLength());

						// Keep going until a record batch
						return self.next();
					}
					return IpcError.InvalidDictionaryBatchHeader;
				},
				.RecordBatch => {
					if (message.Header()) |header| {
						const record = try record_batch.RecordBatch
							.init(header.bytes, header.pos)
							.Unpack(.{ .allocator = allocator });
						return try self.readBatch(record, message.BodyLength());
					}
					return IpcError.InvalidRecordBatchHeader;
				}
			}
		}
		return null;
	}
};

const sample = @import("../sample.zig");

fn testEquals(arr1: *Array, arr2: *Array) !void {
	errdefer {
		std.debug.print("expected: \n", .{});
		arr1.print();

		std.debug.print("actual: \n", .{});
		arr2.print();
	}
	try std.testing.expectEqual(arr1.tag, arr2.tag);
	try std.testing.expectEqualStrings(arr1.name, arr2.name);
	try std.testing.expectEqual(arr1.length, arr2.length);
	try std.testing.expectEqual(arr1.null_count, arr2.null_count);
	for (0..arr1.bufs.len) |i| {
		try std.testing.expectEqualSlices(u8, arr1.bufs[i], arr2.bufs[i]);
	}
	try std.testing.expectEqual(arr1.children.len, arr2.children.len);
	for (0..arr1.children.len) |i| {
		try testEquals(arr1.children[i], arr2.children[i]);
	}
}

test "example file path" {
	std.testing.log_level = .debug;
	var ipc_reader = try RecordBatchReader.initFilePath(std.testing.allocator, "./example.arrow");
	defer ipc_reader.deinit();
	const expected = try sample.sampleArray(std.testing.allocator);
	try expected.toRecordBatch("record batch");
	defer expected.deinit();
	try std.testing.expectEqual(expected.children.len, ipc_reader.schema.fields.items.len);

	var n_batches: usize = 0;
	while (try ipc_reader.next()) |rb| {
		defer rb.deinit();
		try testEquals(expected, rb);
		n_batches += 1;
	}
	try std.testing.expectEqual(@as(usize, 1), n_batches);
}

test "tickers file" {
	std.testing.log_level = .debug;
	var ipc_reader = try RecordBatchReader.initFilePath(std.testing.allocator, "./tickers.arrow");
	defer ipc_reader.deinit();
	try std.testing.expectEqual(@as(usize, 22), ipc_reader.schema.fields.items.len);

	var n_batches: usize = 0;
	while (try ipc_reader.next()) |rb| {
		defer rb.deinit();
		n_batches += 1;
	}
	try std.testing.expectEqual(@as(usize, 1), n_batches);
}