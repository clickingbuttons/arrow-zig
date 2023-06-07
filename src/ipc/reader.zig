const std = @import("std");
const lz4 = @import("lz4");
const shared = @import("shared.zig");
const Array = @import("../array/array.zig").Array;

const footer_mod = @import("./gen/Footer.fb.zig");
const Message = @import("./gen/Message.fb.zig").Message;
const Schema = @import("./gen/Schema.fb.zig").Schema;
const RecordBatch = @import("./gen/RecordBatch.fb.zig").RecordBatch;
const DictionaryBatch = @import("./gen/DictionaryBatch.fb.zig").DictionaryBatch;
const BodyCompression = @import("./gen/BodyCompression.fb.zig").BodyCompression;
const Field = @import("./gen/Field.fb.zig").Field;
const FieldNode = @import("./gen/FieldNode.fb.zig").FieldNode;
const FieldType = @import("./gen/Type.fb.zig").Type;
const Footer = footer_mod.Footer;
const FooterT = footer_mod.FooterT;

const Allocator = std.mem.Allocator;
const log = shared.log;
const IpcError = error {
	InvalidMagicLen,
	InvalidMagic,
	InvalidContinuation,
	InvalidMessageType,
	InvalidSchemaMesssage,
	MissingSchemaMessage,
	InvalidRecordBatch,
	InvalidRecordBatchHeader,
	InvalidDictionaryBatchHeader,
	DictNotFound,
	NoDictionaryData,
	InvalidNumDictionaryBuffers,
	InvalidFooterLen,
} || shared.IpcError;

/// Reads messages out of an IPC stream.
pub fn RecordBatchIterator(comptime ReaderType: type) type {
	return struct {
		const Self = @This();
		const Dictionaries = std.AutoHashMap(i64, struct {
			length: usize,
			null_count: usize,
			buffers: [3]std.ArrayList(u8),

			pub fn deinit(self: @This()) void {
				for (self.buffers) |b| b.deinit();
			}
		});

		allocator: Allocator,
		arena: std.heap.ArenaAllocator,
		source: ReaderType,
		schema: Schema,
		n_fields: usize,
		n_buffers: usize,

		node_index: usize = 0,
		buffer_index: usize = 0,
		dictionaries: Dictionaries,

		pub fn init(allocator: Allocator, source: ReaderType) !Self {
			var res = Self {
				.allocator = allocator,
				.arena = std.heap.ArenaAllocator.init(allocator),
				.source = source,
				.schema = undefined,
				.n_fields = undefined,
				.n_buffers = undefined,
				.dictionaries = Dictionaries.init(allocator),
			};
			errdefer res.arena.deinit();
			res.schema = try res.readSchema();
			res.n_fields = shared.nFields(res.schema);
			res.n_buffers = try shared.nBuffers(res.schema);
			return res;
		}

		pub fn deinit(self: *Self) void {
			var iter = self.dictionaries.valueIterator();
			while (iter.next()) |d| d.deinit();
			self.dictionaries.deinit();
			self.arena.deinit();
		}

		fn readMessageLen(self: *Self) !usize {
			// > This component was introduced in version 0.15.0 in part to address the 8-byte alignment
			// > requirement of Flatbuffers.
			var res = try self.source.reader().readIntLittle(shared.MessageLen);
			if (res == shared.continuation) return self.readMessageLen();

			return @intCast(usize, res);
		}

		pub fn readMessage(self: *Self) !?Message {
			// <continuation: 0xFFFFFFFF> (optional)
			// <metadata_size: int32>
			// <metadata_flatbuffer: bytes>
			// <padding to 8 byte boundary>
			// <message body>
			const message_len = try self.readMessageLen();
			if (message_len == 0) return null; // EOS

			const allocator = self.arena.allocator();
			var message_buf = try allocator.alloc(u8, message_len);
			errdefer allocator.free(message_buf);
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
			const res = try self.arena.allocator().alignedAlloc(u8, shared.buffer_alignment, real_size);
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
				return if (message.Header()) |header|
					Schema.init(header.bytes, header.pos)
				else
					IpcError.InvalidSchemaMesssage;
			}

			return IpcError.MissingSchemaMessage;
		}

		fn readField(self: *Self, buffers: []Array.Buffer, batch: RecordBatch, field: Field) !*Array {
			const allocator = self.allocator;
			const tag = try shared.toTag(field);
			// log.debug("read field \"{s}\" type {any} n_children {d}", .{ field.name, tag, field.children.items.len });

			const node = batch.Nodes(self.node_index).?;
			var res = try allocator.create(Array);
			res.* = .{
				.tag = tag,
				.name = field.Name(),
				.allocator = allocator,
				.length = @intCast(usize, node.Length()),
				.null_count = @intCast(usize, node.NullCount()),
				.children = try allocator.alloc(*Array, field.ChildrenLen()),
			};
			self.node_index += 1;

			for (0..res.tag.abiLayout().nBuffers()) |i| {
				res.buffers[i] = buffers[self.buffer_index];
				self.buffer_index += 1;
			}

			for (0..field.ChildrenLen()) |i| {
				res.children[i] = try self.readField(buffers, batch, field.Children(i).?);
			}
			if (field.Dictionary()) |d| {
				// log.debug("read field \"{s}\" dictionary {d}", .{ field.name, d.id });
				if (self.dictionaries.get(d.Id())) |v| {
					// TODO: refcount dictionary instead of copying out
					var dict_values = try allocator.create(Array);
					dict_values.* = .{
						.tag = try shared.toFieldTag(field),
						.name = "dict values",
						.allocator = allocator,
						.length = v.length,
						.null_count = v.null_count,
						.buffers = .{
							try allocator.alignedAlloc(u8, shared.buffer_alignment, v.buffers[0].items.len),
							try allocator.alignedAlloc(u8, shared.buffer_alignment, v.buffers[1].items.len),
							try allocator.alignedAlloc(u8, shared.buffer_alignment, v.buffers[2].items.len),
						},
						.children = &.{},
					};
					@memcpy(dict_values.buffers[0], v.buffers[0].items);
					@memcpy(dict_values.buffers[1], v.buffers[1].items);
					@memcpy(dict_values.buffers[2], v.buffers[2].items);
					res.children = try allocator.alloc(*Array, 1);
					res.children[0] = dict_values;
				} else {
					return IpcError.DictNotFound;
				}
			}
			return res;
		}

		fn readBuffer(self: *Self, allocator: Allocator, size: usize, compression: ?BodyCompression) !Array.Buffer {
			// Undocumented, but whatever :)
			if (size == 0) return try allocator.alignedAlloc(u8, shared.buffer_alignment, 0);
			// > Each constituent buffer is first compressed with the indicated
			// > compressor, and then written with the uncompressed length in the first 8
			// > bytes as a 64-bit little-endian signed integer followed by the compressed
			// > buffer bytes (and then padding as required by the protocol). The
			// > uncompressed length may be set to -1 to indicate that the data that
			// > follows is not compressed, which can be useful for cases where
			// > compression does not yield appreciable savings.
			const uncompressed_size = if (compression != null) brk: {
					const res = try self.source.reader().readIntLittle(i64);
					break :brk if (res == -1) size else @intCast(usize, res);
				} else size;
			var res = try allocator.alignedAlloc(u8, shared.buffer_alignment, uncompressed_size);
			errdefer allocator.free(res);
			var n_read: ?usize = null;

			if (compression) |c| {
				switch (c.Codec()) {
					.LZ4_FRAME => {
						var stream = lz4.decompressStream(self.arena.allocator(), self.source.reader());
						defer stream.deinit();
						n_read = try stream.read(res);
					},
					.ZSTD => {
						var stream = std.compress.zstd.decompressStream(self.arena.allocator(), self.source.reader());
						defer stream.deinit();
						n_read = try stream.read(res);
					},
				}
			} else {
				n_read = try self.source.reader().read(res);
			}

			if (res.len != n_read) {
				log.err("expected {d} bytes in record batch body, got {any}", .{ res.len, n_read });
				return IpcError.InvalidRecordBatch;
			}
			return res;
		}

		fn readBuffers(self: *Self, allocator: Allocator, batch: RecordBatch, body_len: i64) ![]Array.Buffer {
			var i: usize = 0;
			var res = try allocator.alloc(Array.Buffer, batch.BuffersLen());
			errdefer {
				for (0..i) |j| allocator.free(res[j]);
				allocator.free(res);
			}

			for (0..batch.BuffersLen()) |j| {
				const info = batch.Buffers(j).?;
				const size = @intCast(usize, info.Length());
				res[i] = try self.readBuffer(allocator, size, batch.Compression());

				const next_offset = if (i == res.len - 1) body_len else batch.Buffers(i + 1).?.Offset();
				i += 1;

				const seek = next_offset - (info.Offset() + info.Length());
				try self.source.reader().skipBytes(@intCast(u64, seek), .{});
			}

			return res;
		}

		/// Caller owns Array.
		fn readBatch(self: *Self, batch: RecordBatch, body_len: i64) !*Array {
			// https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message
			// > Fields and buffers are flattened by a pre-order depth-first traversal of the fields in the
			// > record batch.
			log.debug("read batch len {d} compression {any}", .{ batch.Length(), batch.Compression() });
			const allocator = self.allocator;

			// Quickly check that the number of buffers and field nodes matches the schema.
			if (batch.BuffersLen() != self.n_buffers) {
				log.warn("skipped reading batch with {d} buffers (schema expects {d})",
					.{ batch.BuffersLen(), self.n_buffers });
				return IpcError.InvalidLen;
			}
			if (batch.NodesLen() != self.n_fields) {
				log.warn("skipped reading batch with {d} fields (schema expects {d})",
					.{ batch.NodesLen(), self.n_fields });
				return IpcError.InvalidLen;
			}

			// Read flattened buffers
			const buffers = try self.readBuffers(allocator, batch, body_len);
			defer allocator.free(buffers);

			// Recursively read tags, name, and buffers into arrays from `schema.fields`
			self.node_index = 0;
			self.buffer_index = 0;
			var children = try allocator.alloc(*Array, self.schema.FieldsLen());
			for (0..self.schema.FieldsLen()) |i| {
				children[i] = try self.readField(buffers, batch, self.schema.Fields(i).?);
			}

			const res = try allocator.create(Array);
			res.* = .{
				.tag = .{ .Struct = .{ .nullable = false } },
				.name = "record batch",
				.allocator = allocator,
				.length = @intCast(usize, batch.Length()),
				.null_count = 0,
				.children = children,
			};
			return res;
		}

		pub fn nextBatch(self: *Self, message: *const Message) !?*Array {
			if (message.Header()) |header| {
				const record = RecordBatch.init(header.bytes, header.pos);
				return try self.readBatch(record, message.BodyLength());
			}
			return IpcError.InvalidRecordBatchHeader;
		}

		fn readDict(self: *Self, dict: DictionaryBatch, body_len: i64) !void {
			// We own the dictionaries due to any message being able to update them. The values are
			// copied out into arrays.
			// An alternative is adding reference counting support to Array. However, that will result in
			// previous record batches referencing that dictionary being mutated which I'd argue is BAD
			// behavior.
			const allocator = self.arena.allocator();

			log.debug("read_dict {d}", .{ dict.Id() });
			if (dict.Data() == null) return IpcError.NoDictionaryData;
			const batch = dict.Data().?;
			const n_actual: usize = batch.BuffersLen();
			if (n_actual > 3) {
				log.warn("expected dictionary data to have 3 or fewer buffers, got {d}", .{ n_actual });
				return IpcError.InvalidNumDictionaryBuffers;
			}
			const node = batch.Nodes(0).?;
			const buffers = try self.readBuffers(allocator, batch, body_len);
			defer allocator.free(buffers);

			if (dict.IsDelta()) {
				if (self.dictionaries.getPtr(dict.Id())) |existing| {
					for (0..buffers.len) |i| try existing.buffers[i].appendSlice(buffers[i]);
				} else {
					log.warn("ignoring delta for non-existant dictionary {d}", .{ dict.Id() });
				}
			} else {
				var mutable_bufs: [3]std.ArrayList(u8) = undefined;
				for (0..mutable_bufs.len) |i| mutable_bufs[i] = std.ArrayList(u8).init(allocator);
				for (0..buffers.len) |i| try mutable_bufs[i].appendSlice(buffers[i]);

				if (try self.dictionaries.fetchPut(dict.Id(), .{
						.length = @intCast(usize, node.Length()),
						.null_count = @intCast(usize, node.NullCount()),
						.buffers = mutable_bufs,
					})) |existing| {
					log.warn("spec does not support replacing dictionary for dictionary {d}", .{ dict.Id() });
					existing.value.deinit();
				}
			}
		}

		pub fn nextDict(self: *Self, message: *const Message) !void {
			if (message.Header()) |header| {
				const dict = DictionaryBatch.init(header.bytes, header.pos);
				try self.readDict(dict, message.BodyLength());
			} else {
				return IpcError.InvalidDictionaryBatchHeader;
			}
		}

		/// Caller owns Array.
		pub fn next(self: *Self) !?*Array {
			if (try self.readMessage()) |message| {
				switch (message.HeaderType()) {
					.NONE, .Schema => {
						log.warn("ignoring unexpected message {any}", .{ message.HeaderType() });
						try self.source.reader().skipBytes(@intCast(u64, message.BodyLength()), .{});
					},
					.DictionaryBatch => {
						try self.nextDict(&message);
						// Keep going until a record batch
						return self.next();
					},
					.RecordBatch => return try self.nextBatch(&message),
				}
			}
			return null;
		}
	};
}

fn BufferedReader(comptime ReaderType: type) type {
	return std.io.BufferedReader(4096, ReaderType);
}

pub fn recordBatchReader(
	allocator: Allocator,
	reader: anytype
) !RecordBatchIterator(BufferedReader(@TypeOf(reader))) {
	var buffered = std.io.bufferedReader(reader);
	return RecordBatchIterator(BufferedReader(@TypeOf(reader))).init(allocator, buffered);
}

/// Handles file header, footer and strange dictionary requirements. Convienently closes file in .deinit.
const RecordBatchFileReader = struct {
	const Self = @This();
	const Reader = RecordBatchIterator(BufferedReader(std.fs.File.Reader));

	allocator: Allocator,
	file: std.fs.File,
	footer: FooterT,
	reader: Reader,
	batch_index: usize,

	fn readMagic(file: std.fs.File, comptime location: []const u8) !void {
		var maybe_magic: [shared.magic.len]u8 = undefined;
		const n_read = try file.read(&maybe_magic);
		if (shared.magic.len != n_read) {
			log.err("expected {s} magic len {d}, got {d}", .{ location, shared.magic.len, n_read });
			return IpcError.InvalidMagicLen;
		}
		if (!std.mem.eql(u8, shared.magic, &maybe_magic)) {
			log.err("expected {s} magic {s}, got {s}", .{ location, shared.magic, maybe_magic });
			return IpcError.InvalidMagic;
		}
	}

	fn readFooter(allocator: Allocator, file: std.fs.File) !FooterT {
		const FooterSize = i32;
		const seek_back = -(@as(i64, shared.magic.len) + @sizeOf(FooterSize));
		try file.seekFromEnd(seek_back);
		const pos = try file.getPos();
		const footer_len = try file.reader().readIntLittle(FooterSize);
		if (footer_len < 0 or footer_len > pos) {
			log.err("invalid footer len {d}", .{ footer_len });
			return IpcError.InvalidFooterLen;
		}
		try readMagic(file, "end");

		const footer_buf = try allocator.alloc(u8, @intCast(usize, footer_len));
		defer allocator.free(footer_buf);
		try file.seekFromEnd(seek_back - footer_len);
		const n_read = try file.read(footer_buf);
		if (n_read != footer_buf.len) {
			return IpcError.InvalidLen;
		}

		return try Footer.GetRootAs(footer_buf, 0).Unpack(.{ .allocator = allocator });
	}

	fn initReader(allocator: Allocator, file: std.fs.File, footer: FooterT) !Reader {
		try file.seekTo(8); // 8 byte padding for flatbuffers
		// It will read the schema from the streaming format rather than the footer.
		// TODO: validate footer matches
		var reader = try recordBatchReader(allocator, file.reader());

		// From the spec:
		// > In the file format, there is no requirement that dictionary keys should be defined in
		// > a DictionaryBatch before they are used in a RecordBatch, as long as the keys are defined
		// > somewhere in the file.
		// We respect this by reading all dictionaries on init.

		// > Further more, it is invalid to have more than one non-delta dictionary batch per
		// > dictionary ID (i.e. dictionary replacement is not supported).
		// We disrespect this by reading them all anyways. It doesn't matter.

		// > Delta dictionaries are applied in the order they appear in the file footer.
		// We respect this.

		for (footer.dictionaries.items) |d| {
			const offset = @intCast(usize, d.offset) - 4;
			try file.seekTo(offset);
			if (try reader.readMessage()) |message| {
				try reader.nextDict(&message);
			} else {
				log.warn("invalid dictionary message at offset {d}", .{ offset });
			}
		}

		return reader;
	}

	pub fn init(allocator: Allocator, fname: []const u8) !Self {
		var file = try std.fs.cwd().openFile(fname, .{});

		try readMagic(file, "start");
		var footer = try readFooter(allocator, file);
		errdefer footer.deinit(allocator);

		return .{
			.allocator = allocator,
			.file = file,
			.footer = footer,
			.reader = try initReader(allocator, file, footer),
			.batch_index = 0,
		};
	}

	pub fn deinit(self: *Self) void {
		self.reader.deinit();
		self.footer.deinit(self.allocator);
		self.file.close();
	}

	pub fn next(self: *Self) !?*Array {
		if (self.batch_index >= self.footer.recordBatches.items.len) return null;

		const rb = self.footer.recordBatches.items[self.batch_index];
		const offset = @intCast(usize, rb.offset) - 4;
		try self.file.seekTo(offset);
		self.batch_index += 1;

		if (try self.reader.readMessage()) |message| return try self.reader.nextBatch(&message);
		log.err("invalid record batch message at offset {d}", .{ offset });
		return IpcError.InvalidRecordBatch;
	}
};

pub fn recordBatchFileReader(allocator: Allocator, fname: []const u8) !RecordBatchFileReader {
	return RecordBatchFileReader.init(allocator, fname);
}

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
	for (0..arr1.buffers.len) |i| try std.testing.expectEqualSlices(u8, arr1.buffers[i], arr2.buffers[i]);
	try std.testing.expectEqual(arr1.children.len, arr2.children.len);
	for (0..arr1.children.len) |i| try testEquals(arr1.children[i], arr2.children[i]);
}

test "read sample file" {
	var ipc_reader = try recordBatchFileReader(std.testing.allocator, "./sample.arrow");
	defer ipc_reader.deinit();

	const expected = try sample.all(std.testing.allocator);
	try expected.toRecordBatch("record batch");
	defer expected.deinit();
	try std.testing.expectEqual(expected.children.len, ipc_reader.reader.schema.FieldsLen());

	var n_batches: usize = 0;
	while (try ipc_reader.next()) |rb| {
		defer rb.deinit();
		try testEquals(expected, rb);
		n_batches += 1;
	}
	try std.testing.expectEqual(@as(usize, 1), n_batches);
}

test "read tickers file" {
	var ipc_reader = try recordBatchFileReader(std.testing.allocator, "./tickers.arrow");
	defer ipc_reader.deinit();

	var n_batches: usize = 0;
	while (try ipc_reader.next()) |rb| {
		defer rb.deinit();
		n_batches += 1;
	}
	try std.testing.expectEqual(@as(usize, 1), n_batches);
}
