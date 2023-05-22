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
};

fn toDictTag(dict: *DictionaryEncoding) !tags.Tag {
	return .{
		.dictionary = .{
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
		.Null => .null,
		.Int => |maybe_i| {
			if (maybe_i) |i| {
				return switch (i.bitWidth) {
					8 => switch (i.is_signed) {
						 true => .{ .i8 = .{ .is_nullable = field.nullable } },
						 false => .{ .u8 = .{ .is_nullable = field.nullable } },
					},
					16 => switch (i.is_signed) {
						 true => .{ .i16 = .{ .is_nullable = field.nullable } },
						 false => .{ .u16 = .{ .is_nullable = field.nullable } },
					},
					32 => switch (i.is_signed) {
						 true => .{ .i32 = .{ .is_nullable = field.nullable } },
						 false => .{ .u32 = .{ .is_nullable = field.nullable } },
					},
					64 => switch (i.is_signed) {
						 true => .{ .i64 = .{ .is_nullable = field.nullable } },
						 false => .{ .u64 = .{ .is_nullable = field.nullable } },
					},
					else => |w| {
						log.warn("int field {s} invalid bit width {d}", .{ field.name, w });
						return IpcError.InvalidBitWidth;
					}
				};
			}
			log.warn("int field {s} missing bit width", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.FloatingPoint => |maybe_f| if (maybe_f) |f| switch (f.precision) {
			.HALF => .{ .f16 = .{ .is_nullable = field.nullable } },
			.SINGLE => .{ .f32 = .{ .is_nullable = field.nullable } },
			.DOUBLE => .{ .f64 = .{ .is_nullable = field.nullable } },
		} else {
			log.warn("float field {s} missing precision", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.Binary => .{ .binary = .{ .is_large = false, .is_utf8 = false } },
		.LargeBinary => .{ .binary = .{ .is_large = true, .is_utf8 = false } },
		.Utf8 => .{ .binary = .{ .is_large = false, .is_utf8 = true } },
		.LargeUtf8 => .{ .binary = .{ .is_large = true, .is_utf8 = true } },
		.Bool => .{ .bool = .{ .is_nullable = field.nullable } },
		// .Decimal: ?*DecimalT,
		// .Date: ?*DateT,
		// .Time: ?*TimeT,
		// .Timestamp: ?*TimestampT,
		// .Interval: ?*IntervalT,
		.List => .{ .list = .{ .is_nullable = field.nullable, .is_large = false } },
		.LargeList => .{ .list = .{ .is_nullable = field.nullable, .is_large = true } },
		.Struct_ => .{ .struct_ = .{ .is_nullable = field.nullable } },
		.Union => |maybe_u| if (maybe_u) |u| switch (u.mode) {
			.Sparse => .{ .union_ = .{ .is_nullable = field.nullable, .is_dense = false } },
			.Dense => .{ .union_ = .{ .is_nullable = field.nullable, .is_dense = true } },
		} else {
			log.warn("union field {s} missing mode", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.FixedSizeBinary => |maybe_b| if (maybe_b) |b| .{
			.list_fixed = .{
				.is_nullable = field.nullable,
				.fixed_len = @intCast(i16, b.byteWidth),
				.is_large = false,
			}
		} else {
			log.warn("fixed size binary field {s} missing byte width", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		.FixedSizeList => |maybe_f| if (maybe_f) |f| .{
			.list_fixed = .{
				.is_nullable = field.nullable,
				.fixed_len = @intCast(i16, f.listSize),
				.is_large = false,
			}
		} else {
			log.warn("fixed size list field {s} missing fixed length", .{ field.name });
			return IpcError.InvalidFieldTag;
		},
		// .Map: ?*MapT,
		// .Duration: ?*DurationT,
		// .NONE: void,
		// .RunEndEncoded: ?*RunEndEncodedT,
		else => .null,
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
	const Dictionaries = std.AutoHashMap(i64, *Array);

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

	fn readBuffers(self: *Self, batch: RecordBatch, body_len: i64) ![]BufferT {
		const allocator = self.arena.allocator();

		var buffers = try allocator.alloc(BufferT, batch.buffers.items.len);
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
		const allocator = self.arena.allocator();
		log.debug("read field \"{s}\" n_children {d}", .{ field.name, field.children.items.len });

		var res = try allocator.create(Array);
		res.* = .{
			.tag = try toTag(field),
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
				v.tag = try toFieldTag(field);
				res.children = try allocator.alloc(*Array, 1);
				res.children[0] = v;
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
		const allocator = self.arena.allocator();

		// Quickly check that the number of buffers and field nodes matches the schema.
		if (batch.buffers.items.len != self.n_buffers) {
			log.warn("skipped batch with {d} buffers (schema expects {d})",
				.{ batch.buffers.items.len, self.n_buffers });
		}
		if (batch.nodes.items.len != self.n_fields) {
			log.warn("skipped batch with {d} fields (schema expects {d})",
				.{ batch.nodes.items.len, self.n_fields });
		}

		// Read flattened buffers
		const buffers = try self.readBuffers(batch, body_len);

		// Recursively read tags, name, and buffers into arrays from `schema.fields`
		self.node_index = 0;
		self.buffer_index = 0;
		var children = try allocator.alloc(*Array, self.schema.fields.items.len);
		for (self.schema.fields.items, 0..) |f, i| {
			children[i] = try self.readField(buffers, batch.nodes.items, f);
		}

		const res = try allocator.create(Array);
		res.* = .{
			.tag = .{ .struct_ = .{ .is_nullable = false } },
			.name = "record batch",
			.allocator = allocator,
			.length = @intCast(usize, batch.length),
			.null_count = 0,
			.bufs = .{ &.{}, &.{}, &.{} },
			.children = children,
		};
		return res;
	}

	fn readDict(self: *Self, dict: DictionaryBatch, body_len: i64) !void {
		const allocator = self.arena.allocator();

		log.debug("read_dict {d}", .{dict.id});

		
		const batch = if (dict.data) |d| d.* else return IpcError.NoDictionaryData;
		const node = batch.nodes.items[0];

		const dict_values = try allocator.create(Array);
		dict_values.* = .{
			.tag = .null, // Schema has the info for this but not the ID.
			.name = "dict values",
			.allocator = allocator,
			.length = @intCast(usize, node.length),
			.null_count = @intCast(usize, node.null_count),
			.bufs = .{ &.{}, &.{}, &.{} },
			.children = &.{},
		};

		const buffers = try self.readBuffers(batch, body_len);
		for (0..buffers.len) |i| {
			dict_values.bufs[i] = buffers[i];
		}

		try self.dictionaries.put(dict.id, dict_values);
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

// pub fn readFile(file: std.fs.File) !RecordBatchReader {
	// var reader = source.reader();
	// try checkMagic(reader);
	// const end = try source.getEndPos();
	// try source.seekTo(end - magic.len);
	// try checkMagic(reader);

	// const FooterSize = i32;
	// var footer_size: FooterSize = 0;
	// try source.seekTo(end - magic.len - @sizeOf(FooterSize));
	// footer_size = try reader.readIntLittle(FooterSize);
	// const footer_len = @intCast(usize, footer_size);

	// var buf = try allocator.alloc(u8, footer_len);
	// try source.seekTo(end - magic.len - @sizeOf(FooterSize) - footer_len);
	// const n_read = try reader.read(buf);
	// if (n_read != buf.len) {
	// 	return IpcError.InvalidLen;
	// }

	// const footer = Footer.GetRootAs(buf, 0);
	// std.debug.print("footer {any}\n", .{ footer.Schema_().? });
// }

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
		// defer record_batch.deinit();
		try testEquals(expected, rb);
		n_batches += 1;
	}
	try std.testing.expectEqual(@as(usize, 1), n_batches);
}
