const std = @import("std");
const flatbuffers = @import("flatbuffers");
const tags = @import("../tags.zig");
const Array = @import("../array/array.zig").Array;
const shared = @import("shared.zig");
const TypeId = @import("../array/union.zig").TypeId;
const flat = @import("./gen/lib.zig");

const log = shared.log;
const Allocator = std.mem.Allocator;
const FieldNode = flat.FieldNode;
const Message = flat.Message;
const Buffer = flat.Buffer;
const Block = flat.Block;
const Schema = flat.Schema;
const IpcError = error{
    ArrayNotDictionary,
} || shared.IpcError;
// This is what the C++ impl does "for flatbuffers." Not sure how valid that is...
const message_alignment = 8;

fn getFieldNodes(accumulator: *std.ArrayList(FieldNode), array: *Array) !void {
    try accumulator.append(FieldNode{
        .length = @bitCast(array.length),
        .null_count = @bitCast(array.null_count),
    });
    if (array.tag != .Dictionary) for (array.children) |c| try getFieldNodes(accumulator, c);
}

test "getFieldNodes root" {
    const allocator = std.testing.allocator;
    const batch = try sample.all(allocator);
    defer batch.deinit();

    var nodes = std.ArrayList(FieldNode).init(allocator);
    defer nodes.deinit();
    for (batch.children) |c| try getFieldNodes(&nodes, c);

    const expected_fields = &[_]FieldNode{
        .{ .length = 4, .null_count = 1 },
        .{ .length = 4, .null_count = 1 },
        .{ .length = 12, .null_count = 0 },
        .{ .length = 4, .null_count = 1 },
        .{ .length = 4, .null_count = 1 },
        .{ .length = 9, .null_count = 0 },
        .{ .length = 4, .null_count = 1 },
        .{ .length = 12, .null_count = 0 },
        .{ .length = 4, .null_count = 1 },
        .{ .length = 4, .null_count = 2 },
        .{ .length = 4, .null_count = 1 },
        .{ .length = 4, .null_count = 0 },
        .{ .length = 3, .null_count = 1 },
        .{ .length = 1, .null_count = 0 },
        .{ .length = 4, .null_count = 0 },
        .{ .length = 4, .null_count = 2 },
        .{ .length = 4, .null_count = 3 },
        .{ .length = 4, .null_count = 1 },
        .{ .length = 4, .null_count = 1 },
        .{ .length = 4, .null_count = 0 },
        .{ .length = 4, .null_count = 0 },
        .{ .length = 4, .null_count = 1 },
    };

    const schema = try Schema.initFromArray(allocator, batch);
    defer schema.deinit(allocator);
    const n_fields = schema.nFields();
    try std.testing.expectEqual(expected_fields.len, n_fields);

    try std.testing.expectEqualSlices(FieldNode, expected_fields, nodes.items);
}

inline fn getPadding(comptime alignment: usize, n: usize) usize {
    const mod = @mod(n, alignment);
    if (mod != 0) return alignment - mod;
    return 0;
}

fn writeBuffers(
    array: *Array,
    writer_: anytype,
    accumulator: ?*std.ArrayList(Buffer),
) !usize {
    const commit = @typeInfo(@TypeOf(writer_)) == .Struct;

    var res: usize = 0;

    for (0..array.tag.abiLayout().nBuffers()) |i| {
        const b = array.buffers[i];
        if (commit) try writer_.writeAll(b);
        res += b.len;
        // The C++ implementation uses 8 byte alignment here, but we use 64 to prevent copying
        // for 64-bit alignment libraries.
        const n_padding = getPadding(Array.buffer_alignment, b.len);
        res += n_padding;
        if (commit) for (0..n_padding) |_| try writer_.writeByte(0);

        if (accumulator) |a| {
            var offset = brk: {
                if (a.items.len == 0) break :brk 0;
                const last = a.getLast();
                const padding: i64 = @bitCast(getPadding(Array.buffer_alignment, @bitCast(last.length)));
                break :brk last.offset + last.length + padding;
            };
            try a.append(Buffer{
                .offset = offset,
                .length = @bitCast(b.len),
            });
        }
    }
    if (array.tag != .Dictionary) {
        for (array.children) |c| res += try writeBuffers(c, writer_, accumulator);
    }
    return res;
}

test "writeBuffer dict" {
    const allocator = std.testing.allocator;
    const dict = try sample.dict(allocator);
    defer dict.deinit();

    var buffers = std.ArrayList(Buffer).init(allocator);
    defer buffers.deinit();
    _ = try writeBuffers(dict.children[0], void, &buffers);

    const expected_buffers = &[_]Buffer{
        .{ .offset = 0, .length = 0 },
        .{ .offset = 0, .length = 16 },
        .{ .offset = 64, .length = 16 },
    };

    const schema = try Schema.initFromArray(allocator, dict);
    defer schema.deinit(allocator);
    const n_buffers = try schema.nBuffers();
    try std.testing.expectEqual(expected_buffers.len, n_buffers);

    try std.testing.expectEqualSlices(Buffer, expected_buffers, buffers.items);
}

test "writeBuffer root" {
    const allocator = std.testing.allocator;
    const batch = try sample.all(allocator);
    defer batch.deinit();

    var buffers = std.ArrayList(Buffer).init(allocator);
    defer buffers.deinit();

    for (batch.children) |c| _ = try writeBuffers(c, void, &buffers);

    const expected_buffers = &[_]Buffer{
        .{ .offset = 0, .length = 1 },
        .{ .offset = 64, .length = 8 },
        .{ .offset = 128, .length = 1 },
        .{ .offset = 192, .length = 0 },
        .{ .offset = 192, .length = 24 },
        .{ .offset = 256, .length = 1 },
        .{ .offset = 320, .length = 20 },
        .{ .offset = 384, .length = 18 },
        .{ .offset = 448, .length = 1 },
        .{ .offset = 512, .length = 20 },
        .{ .offset = 576, .length = 0 },
        .{ .offset = 576, .length = 18 },
        .{ .offset = 640, .length = 1 },
        .{ .offset = 704, .length = 0 },
        .{ .offset = 704, .length = 24 },
        .{ .offset = 768, .length = 1 },
        .{ .offset = 832, .length = 1 },
        .{ .offset = 896, .length = 16 },
        .{ .offset = 960, .length = 1 },
        .{ .offset = 1024, .length = 32 },
        .{ .offset = 1088, .length = 4 },
        .{ .offset = 1152, .length = 16 },
        .{ .offset = 1216, .length = 1 },
        .{ .offset = 1280, .length = 12 },
        .{ .offset = 1344, .length = 0 },
        .{ .offset = 1344, .length = 4 },
        .{ .offset = 1408, .length = 4 },
        .{ .offset = 1472, .length = 1 },
        .{ .offset = 1536, .length = 16 },
        .{ .offset = 1600, .length = 1 },
        .{ .offset = 1664, .length = 16 },
        .{ .offset = 1728, .length = 1 },
        .{ .offset = 1792, .length = 4 },
        .{ .offset = 1856, .length = 1 },
        .{ .offset = 1920, .length = 20 },
        .{ .offset = 1984, .length = 0 },
        .{ .offset = 1984, .length = 0 },
        .{ .offset = 1984, .length = 20 },
        .{ .offset = 2048, .length = 20 },
        .{ .offset = 2112, .length = 1 },
        .{ .offset = 2176, .length = 16 },
    };

    const schema = try Schema.initFromArray(allocator, batch);
    defer schema.deinit(allocator);
    const n_buffers = try schema.nBuffers();
    try std.testing.expectEqual(expected_buffers.len, n_buffers);

    try std.testing.expectEqualSlices(Buffer, expected_buffers, buffers.items);
}

pub fn Writer(comptime WriterType: type) type {
    return struct {
        const Self = @This();
        const Writer_ = std.io.CountingWriter(WriterType);

        allocator: Allocator,
        dest: Writer_,
        dict_id: i64 = 0,
        message_i: usize = 0,

        pub fn init(allocator: Allocator, dest: WriterType) !Self {
            return .{
                .allocator = allocator,
                .dest = std.io.countingWriter(dest),
            };
        }

        /// Writes an aligned message header and returns its offset + length
        fn writeMessage(self: *Self, message: Message) !Block {
            var builder = flatbuffers.Builder.init(self.allocator);
            errdefer builder.deinit();
            const packed_offset = try message.pack(&builder);
            const bytes = try builder.finish(packed_offset);
            defer self.allocator.free(bytes);

            const offset = self.dest.bytes_written;
            const n_padding = getPadding(message_alignment, bytes.len);
            const len: shared.MessageLen = @intCast(bytes.len + n_padding);

            try self.dest.writer().writeIntLittle(shared.MessageLen, shared.continuation);
            try self.dest.writer().writeIntLittle(shared.MessageLen, len);
            try self.dest.writer().writeAll(bytes);
            for (0..n_padding) |_| try self.dest.writer().writeByte(0);

            return .{
                .offset = @bitCast(offset),
                .meta_data_length = @intCast(self.dest.bytes_written - offset),
                .body_length = 0,
            };
        }

        /// Writes a schema message
        pub fn writeSchema(self: *Self, schema: Schema) !Block {
            return try self.writeMessage(.{
                .header = .{ .schema = schema },
                .body_length = 0,
                .custom_metadata = &.{},
            });
        }

        /// Caller owns returned message
        fn getRecordBatch(self: *Self, array: *Array) !flat.RecordBatch {
            const schema = try Schema.initFromArray(self.allocator, array);
            defer schema.deinit(self.allocator);
            const n_fields = schema.nFields();
            const n_buffers = try schema.nBuffers();

            var nodes = try std.ArrayList(FieldNode).initCapacity(self.allocator, n_fields);
            errdefer nodes.deinit();
            for (array.children) |c| try getFieldNodes(&nodes, c);

            var buffers = try std.ArrayList(Buffer).initCapacity(self.allocator, n_buffers);
            errdefer buffers.deinit();
            for (array.children) |c| _ = try writeBuffers(c, void, &buffers);

            return .{
                .length = @bitCast(array.length),
                .nodes = try nodes.toOwnedSlice(),
                .buffers = try buffers.toOwnedSlice(),
            };
        }

        /// Writes a record batch message
        pub fn writeBatch(self: *Self, array: *Array) !Block {
            const message = Message{
                .header = .{ .record_batch = try self.getRecordBatch(array) },
                .body_length = @bitCast(try writeBuffers(array, void, null)),
                .custom_metadata = &.{},
            };
            defer message.deinit(self.allocator);

            var res = try self.writeMessage(message);
            res.body_length = @bitCast(try writeBuffers(array, self.dest.writer(), null));
            return res;
        }

        /// Writes a dictionary batch message
        pub fn writeDict(self: *Self, array: *Array) !Block {
            if (array.tag != .Dictionary) {
                log.warn("called writeDict on non-dictionary array {s}", .{array.name});
                return IpcError.ArrayNotDictionary;
            }
            const record_batch_message = try self.getRecordBatch(array);
            // for (record_batch_message.nodes) |n| log.debug("write dict {any}", .{n});
            // for (record_batch_message.buffers) |n| log.debug("write dict {any}", .{n});
            const dict = array.children[0];
            const message = Message{
                .header = .{ .dictionary_batch = flat.DictionaryBatch{
                    .id = self.dict_id,
                    .data = record_batch_message,
                    .is_delta = false,
                } },
                .body_length = @bitCast(try writeBuffers(dict, void, null)),
                .custom_metadata = &.{},
            };
            defer message.deinit(self.allocator);

            var res = try self.writeMessage(message);
            res.body_length = @bitCast(try writeBuffers(dict, self.dest.writer(), null));
            self.dict_id += 1;
            return res;
        }
    };
}

fn BufferedWriter(comptime WriterType: type) type {
    return std.io.BufferedWriter(4096, WriterType);
}

pub fn writer(
    allocator: Allocator,
    writer_: anytype,
) !Writer(BufferedWriter(@TypeOf(writer_))) {
    var buffered = std.io.bufferedWriter(writer_);
    return Writer(BufferedWriter(@TypeOf(writer_))).init(allocator, buffered);
}

/// Handles file header and footer. Convienently closes file in .deinit.
const FileWriter = struct {
    const Self = @This();
    const WriterType = Writer(BufferedWriter(std.fs.File.Writer));
    const BlockList = std.ArrayList(Block);

    allocator: Allocator,
    file: std.fs.File,
    writer: WriterType,
    schema: ?Schema = null,
    dictionaries: BlockList,
    record_batches: BlockList,

    fn writeMagic(self: *Self, comptime is_start: bool) !void {
        try self.writer.dest.writer().writeAll(shared.magic);
        if (is_start) try self.writer.dest.writer().writeAll("\x00" ** (8 - shared.magic.len));
    }

    pub fn init(allocator: Allocator, fname: []const u8) !Self {
        var file = try std.fs.cwd().createFile(fname, .{});
        errdefer file.close();

        var res = Self{
            .allocator = allocator,
            .file = file,
            .writer = try writer(allocator, file.writer()),
            .dictionaries = BlockList.init(allocator),
            .record_batches = BlockList.init(allocator),
        };
        try res.writeMagic(true);

        return res;
    }

    pub fn deinit(self: *Self) void {
        self.file.close();
        if (self.schema) |s| s.deinit(self.allocator);
        self.dictionaries.deinit();
        self.record_batches.deinit();
    }

    /// Caller owns returned slice
    fn getFooter(self: *Self) ![]const u8 {
        const footer = flat.Footer{
            .schema = self.schema,
            .dictionaries = self.dictionaries.items,
            .record_batches = self.record_batches.items,
            .custom_metadata = &.{},
        };
        // log.debug("footer {any} {any}", .{ footer.dictionaries, footer.record_batches });
        var builder = flatbuffers.Builder.init(self.allocator);
        errdefer builder.deinit();
        const offset = try footer.pack(&builder);
        return try builder.finish(offset);
    }

    fn writeDicts(self: *Self, acc: *BlockList, array: *Array) !void {
        if (array.tag == .Dictionary) {
            const block = try self.writer.writeDict(array);
            try acc.append(block);
        }
        for (array.children) |c| try self.writeDicts(acc, c);
    }

    /// Writes schema and dictionary batches if required. Then writes record batch.
    pub fn write(self: *Self, array: *Array) !void {
        if (self.schema == null) {
            self.schema = try Schema.initFromArray(self.allocator, array);
            _ = try self.writer.writeSchema(self.schema.?);
            try self.writeDicts(&self.dictionaries, array);
        }

        const record_batch = try self.writer.writeBatch(array);
        try self.record_batches.append(record_batch);
    }

    /// Writes footer and sync to disk.
    pub fn finish(self: *Self) !void {
        const footer = try self.getFooter();
        defer self.allocator.free(footer);
        try self.writer.dest.writer().writeAll(footer);

        const len: shared.MessageLen = @intCast(footer.len);
        try self.writer.dest.writer().writeIntLittle(shared.MessageLen, len);

        try self.writeMagic(false);

        try self.writer.dest.child_stream.flush();
        try self.file.sync();
    }
};

pub fn fileWriter(allocator: Allocator, fname: []const u8) !FileWriter {
    return FileWriter.init(allocator, fname);
}

const reader = @import("./reader.zig");
const sample = @import("../sample.zig");

test "write and read sample file" {
    const batch = try sample.all(std.testing.allocator);
    try batch.toRecordBatch("record batch");
    defer batch.deinit();

    const fname = "./testdata/sample_written.arrow";
    var ipc_writer = try fileWriter(std.testing.allocator, fname);
    defer ipc_writer.deinit();
    try ipc_writer.write(batch);
    try ipc_writer.finish();

    var ipc_reader = try reader.fileReader(std.testing.allocator, fname);
    defer ipc_reader.deinit();
    var n_batches: usize = 0;
    while (try ipc_reader.nextBatch()) |rb| {
        defer rb.deinit();
        try reader.testEquals(batch, rb);
        n_batches += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), n_batches);
}
