const std = @import("std");
const Footer = @import("./gen/Footer.fb.zig").Footer;
const Message = @import("./gen/Message.fb.zig").Message;
const Schema = @import("./gen/Schema.fb.zig").Schema;
const RecordBatch = @import("./gen/RecordBatch.fb.zig").RecordBatch;
const Array = @import("../array/array.zig").Array;
const FieldType = @import("./gen/Type.fb.zig").TypeT;
const Field = @import("./gen/Field.fb.zig").Field;
const tags = @import("../tags.zig");
const abi = @import("../abi.zig");

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

const RecordBatchReader = struct {
	allocator: std.mem.Allocator,
	arena: std.heap.ArenaAllocator,
	source: std.io.StreamSource,
	schema: Schema,
	owns_file: bool = false,

	node_index: usize = 0,
	buffer_index: usize = 0,

	const Self = @This();

	pub fn init(allocator: std.mem.Allocator, source: std.io.StreamSource) !Self {
		var res = Self {
			.allocator = allocator,
			.arena = std.heap.ArenaAllocator.init(allocator),
			.source = source,
			.schema = undefined
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
		self.arena.deinit();
	}

	fn checkMagic(self: *Self) !void {
		var buf: [magic.len]u8 = undefined;
		const n_read = try self.source.reader().read(&buf);
		if (n_read != magic.len) {
			return IpcError.InvalidMagicLen;
		}
		if (!std.mem.eql(u8, magic, &buf)) {
			std.debug.print("expected {s}, got {s}\n", .{ magic, buf });
			return IpcError.InvalidMagic;
		}
	}

	fn readMessageLen(self: *Self) !usize {
		// <continuation: 0xFFFFFFFF> THIS IS A LIE.
		// > This component was introduced in version 0.15.0 in part to address the 8-byte alignment
		// requirement of Flatbuffers.
		// Older versions just skip it! :)
		// <metadata_size: int32> (size of metadata_flatbuffer plus padding)
		var res = try self.source.reader().readIntLittle(MessageLen);
		if (res == continuation) {
			return self.readMessageLen();
		}

		return @intCast(usize, res);
	}

	fn readMessage(self: *Self) !?Message {
		// <continuation: 0xFFFFFFFF>
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
		// Seek to end of 8 byte boundary
		try self.source.reader().skipBytes(n_read % 8, .{});
		// std.debug.print("padding {d}\n", .{ n_read % 8 });
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

	fn readBuffer(self: *Self, header: RecordBatch, buf: []u8) []align(abi.BufferAlignment) u8 {
		const buffersLen = @intCast(usize, header.BuffersLen());
		// std.debug.print("buf {d} / {d}\n", .{ self.buffer_index, buffersLen });
		std.debug.assert(self.buffer_index < buffersLen);
		const buffer = header.Buffers(self.buffer_index).?;
		const offset = @intCast(usize, buffer.Offset());
		const len = @intCast(usize, buffer.Length());
		self.buffer_index += 1;
		// std.debug.print("buf {any} offset {d} len {d}\n", .{ buf.ptr, offset, len });
		return @alignCast(8, buf[offset..offset + len]);
	}

	fn readField(self: *Self, header: RecordBatch, buf: []u8, field: Field) !*Array {
		// > Fields and buffers are flattened by a pre-order depth-first traversal of the fields in the
		// > record batch. 
		const allocator = self.arena.allocator();
		const field_type = try FieldType.Unpack(field.TypeType(), field.Type_().?, .{ .allocator = allocator });
		const tag = try toTag(field_type, field.Nullable());

		const layout = tag.abiLayout();
		std.debug.assert(self.node_index < @intCast(usize, header.NodesLen()));
		// std.debug.print("node {d} / {d}\n", .{ self.node_index, header.NodesLen() });
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
			res.bufs[i] = self.readBuffer(header, buf);
		}

		const n_children = @intCast(usize, field.ChildrenLen());
		// std.debug.print("field {s} n_children {d}\n", .{ field.Name(), n_children });
		if (n_children > 0) {
			res.children = try allocator.alloc(*Array, n_children);

			for (0..n_children) |i| {
				res.children[i] = try self.readField(header, buf, field.Children(i).?);
			}
		}
		return res;
	}

	/// Caller owns Array.
	fn readBatch(self: *Self, message: Message) !*Array {
		// https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message
		if (message.Header() == null) {
			return IpcError.InvalidRecordBatchHeader;
		}
		self.node_index = 0;
		self.buffer_index = 0;
		const header = RecordBatch.init(message.Header().?.bytes, message.Header().?.pos);
		const buf = try self.readMessageBody(message.BodyLength());

		const allocator = self.arena.allocator();
		const n_children = @intCast(usize, self.schema.FieldsLen());
		var children = try allocator.alloc(*Array, n_children);

		for (0..n_children) |i| {
			children[i] = try self.readField(header, buf, self.schema.Fields(i).?);
		}

		// std.debug.assert(self.node_index == @intCast(usize, header.NodesLen()));
		// std.debug.assert(self.buffer_index == @intCast(usize, header.BuffersLen()));

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

	/// Caller owns Array.
	pub fn next(self: *Self) !?*Array {
		if (try self.readMessage()) |message| {
			switch (message.HeaderType()) {
				.NONE => return IpcError.InvalidMessageType, 
				.Schema => return IpcError.InvalidMessageType,
				.DictionaryBatch => {
					// std.debug.print("got a dict batch {any}\n", .{ message.BodyLength() });
					const dict_buf = try self.readMessageBody(message.BodyLength());
					_ = dict_buf;

					// Keep going until a record batch
					return self.next();
				},
				.RecordBatch => return try self.readBatch(message)
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
	// try std.testing.expectEqual(@as(i64, 8), ipc_reader.schema.FieldsLen());
	while (try ipc_reader.next()) |record_batch| {
		// defer record_batch.deinit();
		try testEquals(expected, record_batch);
	}
}
