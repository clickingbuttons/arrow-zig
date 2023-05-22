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
const IpcError = error {
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
	InvalidDictionaryBatchHeader,
	InvalidDictionaryIndexType,
	DictNotFound,
};

fn toTag(tag: FieldType, is_nullable: bool) !tags.Tag {
	return switch(tag) {
		.Null => .null,
		.Int => |i| switch (i.?.bitWidth) {
			8 => switch (i.?.is_signed) {
				 true => .{ .i8 = .{ .is_nullable = is_nullable } },
				 false => .{ .u8 = .{ .is_nullable = is_nullable } },
			},
			16 => switch (i.?.is_signed) {
				 true => .{ .i16 = .{ .is_nullable = is_nullable } },
				 false => .{ .u16 = .{ .is_nullable = is_nullable } },
			},
			32 => switch (i.?.is_signed) {
				 true => .{ .i32 = .{ .is_nullable = is_nullable } },
				 false => .{ .u32 = .{ .is_nullable = is_nullable } },
			},
			64 => switch (i.?.is_signed) {
				 true => .{ .i64 = .{ .is_nullable = is_nullable } },
				 false => .{ .u64 = .{ .is_nullable = is_nullable } },
			},
		 else => IpcError.InvalidBitWidth,
		},
		.FloatingPoint => |f| switch (f.?.precision) {
			.HALF => .{ .f16 = .{ .is_nullable = is_nullable } },
			.SINGLE => .{ .f32 = .{ .is_nullable = is_nullable } },
			.DOUBLE => .{ .f64 = .{ .is_nullable = is_nullable } },
		},
		.Binary => .{ .binary = .{ .is_large = false, .is_utf8 = false } },
		.LargeBinary => .{ .binary = .{ .is_large = true, .is_utf8 = false } },
		.Utf8 => .{ .binary = .{ .is_large = false, .is_utf8 = true } },
		.LargeUtf8 => .{ .binary = .{ .is_large = true, .is_utf8 = true } },
		.Bool => .{ .bool = .{ .is_nullable = is_nullable } },
		// .Decimal: ?*DecimalT,
		// .Date: ?*DateT,
		// .Time: ?*TimeT,
		// .Timestamp: ?*TimestampT,
		// .Interval: ?*IntervalT,
		.List => .{ .list = .{ .is_nullable = is_nullable, .is_large = false } },
		.LargeList => .{ .list = .{ .is_nullable = is_nullable, .is_large = true } },
		.Struct_ => .{ .struct_ = .{ .is_nullable = is_nullable } },
		.Union => |u| switch (u.?.mode) {
			.Sparse => .{ .union_ = .{ .is_nullable = is_nullable, .is_dense = false } },
			.Dense => .{ .union_ = .{ .is_nullable = is_nullable, .is_dense = true } },
		},
		.FixedSizeBinary => |b| .{ .list_fixed = .{
			.is_nullable = is_nullable,
			.fixed_len = @intCast(i16, b.?.byteWidth),
			.is_large = false,
		} },
		.FixedSizeList => |f| .{ .list_fixed = .{
			.is_nullable = is_nullable,
			.fixed_len = @intCast(i16, f.?.listSize),
			.is_large = false,
		} },
		// .Map: ?*MapT,
		// .Duration: ?*DurationT,
		// .NONE: void,
		// .RunEndEncoded: ?*RunEndEncodedT,
		else => .null,
	};
}

fn toDictTag(dict: *DictionaryEncoding) !tags.Tag {
	return .{
		.dictionary = .{
			.index = switch (dict.indexType.?.bitWidth) {
				8 => .i8,
				16 => .i16,
				32 => .i32,
				else => return IpcError.InvalidDictionaryIndexType,
			}
		}
	};
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
	const tag = if (f.dictionary) |d| try toDictTag(d) else try toTag(f.type, f.nullable);
	res += tag.abiLayout().nBuffers();
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
			std.debug.print("record batch ended early\n", .{});
			return IpcError.InvalidRecordBatch;
		}

		return res;
	}

	fn readSchema(self: *Self) !Schema {
		if (try self.readMessage()) |message| {
			if (message.HeaderType() != .Schema) {
				std.debug.print("expected initial schema message, got {any}", .{ message.HeaderType() });
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
			if (try self.source.read(buffers[i]) != buffers[i].len) {
				// std.debug.print("ERROR: expected {d} bytes in record batch, got {d}\n", .{ 
				return IpcError.InvalidRecordBatch;
			}

			const next_offset = if (i == buffers.len - 1) body_len else batch.buffers.items[i + 1].offset;
			const seek = next_offset - (info.offset + info.length);
			std.debug.print("seek {d}\n", .{ seek });
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
		std.debug.print("read field {s} n_children {d}\n", .{ field.name, field.children.items.len });

		var res = try allocator.create(Array);
		res.* = .{
			.tag = if (field.dictionary) |d| try toDictTag(d) else try toTag(field.type, field.nullable),
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
			// std.debug.print("dictionary {any}\n", .{ d.Id() });

			if (self.dictionaries.get(d.id)) |v| {
				v.tag = try toTag(field.type, field.nullable);
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
		std.debug.print("read batch {any}\n", .{ batch });
		const allocator = self.arena.allocator();

		// Quickly check that the number of buffers and field nodes matches the schema.
		if (batch.buffers.items.len != self.n_buffers) {
			std.debug.print("WARN: skipped batch with {d} buffers (schema expects {d})\n",
				.{ batch.buffers.items.len, self.n_buffers });
		}
		if (batch.nodes.items.len != self.n_fields) {
			std.debug.print("WARN: skipped batch with {d} fields (schema expects {d})\n",
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

		std.debug.print("read_dict {any}\n", .{dict});

		const batch = dict.data.?.*;
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
					std.debug.print("WARN: got {any} message\n", .{ message.HeaderType() });
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
