const std = @import("std");
const Footer = @import("./gen/Footer.fb.zig").Footer;
const Message = @import("./gen/Message.fb.zig").Message;
const Schema = @import("./gen/Schema.fb.zig").Schema;
const RecordBatch = @import("./gen/RecordBatch.fb.zig").RecordBatch;
const DictionaryBatch = @import("./gen/DictionaryBatch.fb.zig").DictionaryBatch;
const DictionaryEncoding = @import("./gen/DictionaryEncoding.fb.zig").DictionaryEncoding;
const array = @import("../array/array.zig");
const FieldType = @import("./gen/Type.fb.zig").TypeT;
const Field = @import("./gen/Field.fb.zig").Field;
const tags = @import("../tags.zig");
const abi = @import("../abi.zig");

const Array = array.Array;
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

fn toDictTag(dict: DictionaryEncoding) !tags.Tag {
	return .{
		.dictionary = .{
			.index = switch (dict.IndexType().?.BitWidth()) {
				8 => .i8,
				16 => .i16,
				32 => .i32,
				else => return IpcError.InvalidDictionaryIndexType,
			}
		}
	};
}

const RecordBatchReader = struct {
	const Dictionaries = std.AutoHashMap(i64, *Array);

	allocator: std.mem.Allocator,
	arena: std.heap.ArenaAllocator,
	source: std.io.StreamSource,
	schema: Schema,
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
			.dictionaries = Dictionaries.init(allocator),
		};
		res.schema = try res.readSchema();
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
			// Header is the schema. I figured this out from reading arrow-rs.
			if (message.Header()) |header| {
				return Schema.init(header.bytes, header.pos);
			} else {
				return IpcError.InvalidSchemaMesssage;
			}
		}

		return IpcError.MissingSchemaMessage;
	}

	fn readBuffer(self: *Self, header: RecordBatch, body_len: i64) ![]align(abi.BufferAlignment) u8 {
		const allocator = self.arena.allocator();
		const n_buffers = @intCast(usize, header.BuffersLen());
		std.debug.print("buf {d} / {d}\n", .{ self.buffer_index, n_buffers - 1 });
		std.debug.assert(self.buffer_index < n_buffers);
		const buffer = header.Buffers(self.buffer_index).?;
		const offset = buffer.Offset();
		const len = buffer.Length();
		std.debug.assert(offset + len < body_len);

		if (self.buffer_index > 0) {
			const prev_buffer = header.Buffers(self.buffer_index - 1).?;
			const prev_offset = prev_buffer.Offset();
			const prev_len = prev_buffer.Length();
			try self.source.seekBy(offset - prev_len - prev_offset);
		}
		const buf = try allocator.alignedAlloc(u8, abi.BufferAlignment, @intCast(usize, len));
		if (try self.source.read(buf) != buf.len) {
			std.debug.print("record batch ended early\n", .{});
			return IpcError.InvalidRecordBatch;
		}
		self.buffer_index += 1;
		if (self.buffer_index == n_buffers) {
			const to_seek = body_len - (offset + len);
			std.debug.print("seek end {d}\n", .{ to_seek }); 
			try self.source.seekBy(to_seek);
		}
		std.debug.print("buf {any} offset {d} len {d}\n", .{ buf.ptr, offset, len });
		return buf;
	}

	fn readField(self: *Self, header: RecordBatch, body_len: i64, field: Field) !*Array {
		// > Fields and buffers are flattened by a pre-order depth-first traversal of the fields in the
		// > record batch. 
		const allocator = self.arena.allocator();
		const field_type = try FieldType.Unpack(field.TypeType(), field.Type_().?, .{ .allocator = allocator });
		const tag = if (field.Dictionary()) |d| try toDictTag(d) else try toTag(field_type, field.Nullable());

		const layout = tag.abiLayout();
		std.debug.print("node {d} / {d}\n", .{ self.node_index, header.NodesLen() });
		std.debug.assert(self.node_index < @intCast(usize, header.NodesLen()));
		const node = header.Nodes(self.node_index).?;
		self.node_index += 1;

		var res = try allocator.create(Array);
		res.* = .{
			.tag = tag,
			.name = field.Name(),
			.allocator = allocator,
			.length = @intCast(usize, node.Length()),
			.null_count = @intCast(usize, node.NullCount()),
			.bufs = .{ &.{}, &.{}, &.{} },
			.children = &.{},
		};

		for (0..layout.nBuffers()) |i| {
			res.bufs[i] = try self.readBuffer(header, body_len);
		}

		const n_children = @intCast(usize, field.ChildrenLen());
		std.debug.print("field {s} n_children {d}\n", .{ field.Name(), n_children });
		if (n_children > 0) {
			res.children = try allocator.alloc(*Array, n_children);

			for (0..n_children) |i| {
				res.children[i] = try self.readField(header, body_len, field.Children(i).?);
			}
		}
		if (field.Dictionary()) |d| {
			// std.debug.print("dictionary {any}\n", .{ d.Id() });

			if (self.dictionaries.get(d.Id())) |v| {
				v.tag = try toTag(field_type, field.Nullable());
				res.children = try allocator.alloc(*Array, 1);
				res.children[0] = v;
			} else {
				return IpcError.DictNotFound;
			}
		}
		return res;
	}

	/// Caller owns Array.
	fn readBatch(self: *Self, header: RecordBatch, body_len: i64) !*Array {
		// https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message
		const allocator = self.arena.allocator();

		const n_children = @intCast(usize, self.schema.FieldsLen());
		var children = try allocator.alloc(*Array, n_children);

		for (0..n_children) |i| {
			children[i] = try self.readField(header, body_len, self.schema.Fields(i).?);
		}

		std.debug.assert(self.node_index == @intCast(usize, header.NodesLen()));
		std.debug.assert(self.buffer_index == @intCast(usize, header.BuffersLen()));

		const res = try allocator.create(Array);
		res.* = .{
			.tag = .{ .struct_ = .{ .is_nullable = false } },
			.name = "record batch",
			.allocator = allocator,
			.length = @intCast(usize, header.Length()),
			.null_count = 0,
			.bufs = .{ &.{}, &.{}, &.{} },
			.children = children,
		};
		return res;
	}

	fn readDict(self: *Self, header: DictionaryBatch, body_len: i64) !void {
		const allocator = self.arena.allocator();

		// const f = try DictionaryBatch.Unpack(header, .{ .allocator = allocator });
		// std.debug.print("f {any}\n", .{ f.data.?.nodes });

		const batch = header.Data().?;
		const node = batch.Nodes(0).?;

		const dict_values = try allocator.create(Array);
		dict_values.* = .{
			.tag = .null, // Schema has the info for this.
			.name = "dict values",
			.allocator = allocator,
			.length = @intCast(usize, node.Length()),
			.null_count = @intCast(usize, node.NullCount()),
			.bufs = .{ &.{}, &.{}, &.{} },
			.children = &.{},
		};

		for (0..batch.BuffersLen()) |i| {
			dict_values.bufs[i] = try self.readBuffer(batch, body_len);
		}

		try self.dictionaries.put(header.Id(), dict_values);
	}

	/// Caller owns Array.
	pub fn next(self: *Self) !?*Array {
		if (try self.readMessage()) |message| {
			self.node_index = 0;
			self.buffer_index = 0;
			switch (message.HeaderType()) {
				.NONE => return IpcError.InvalidMessageType, 
				.Schema => return IpcError.InvalidMessageType,
				.DictionaryBatch => {
					if (message.Header() == null) {
						return IpcError.InvalidDictionaryBatchHeader;
					}
					const header = DictionaryBatch.init(message.Header().?.bytes, message.Header().?.pos);
					try self.readDict(header, message.BodyLength());

					// Keep going until a record batch
					return self.next();
				},
				.RecordBatch => {
					if (message.Header() == null) {
						return IpcError.InvalidRecordBatchHeader;
					}
					const header = RecordBatch.init(message.Header().?.bytes, message.Header().?.pos);
					return try self.readBatch(header, message.BodyLength());
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
	try std.testing.expectEqual(@intCast(u32, expected.children.len), ipc_reader.schema.FieldsLen());

	var n_batches: usize = 0;
	while (try ipc_reader.next()) |record_batch| {
		// defer record_batch.deinit();
		try testEquals(expected, record_batch);
		n_batches += 1;
	}
	try std.testing.expectEqual(@as(usize, 1), n_batches);
}
