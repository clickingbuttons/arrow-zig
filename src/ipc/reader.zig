const std = @import("std");
const lz4 = @import("lz4");
const shared = @import("shared.zig");
const flat = @import("./gen/lib.zig");
const Array = @import("../array/array.zig").Array;

const Allocator = std.mem.Allocator;
const log = shared.log;
const IpcError = error{
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
    InvalidNumDictionaryNodes,
    InvalidFooterLen,
} || shared.IpcError;

const Footer = flat.Footer;
const Message = flat.Message;
const RecordBatch = flat.RecordBatch;

const MutableDictionary = struct {
    length: usize,
    null_count: usize,
    buffers: [3]std.ArrayList(u8),

    const Self = @This();

    pub fn deinit(self: Self) void {
        for (self.buffers) |b| b.deinit();
    }
};

/// Reads messages out of an IPC stream.
pub fn Reader(comptime ReaderType: type) type {
    return struct {
        const Self = @This();
        const Dictionaries = std.AutoHashMap(i64, MutableDictionary);

        allocator: Allocator,
        arena: std.heap.ArenaAllocator,
        source: ReaderType,
        schema: ?flat.Schema = null,
        n_fields: usize = 0,
        n_buffers: usize = 0,

        node_index: usize = 0,
        buffer_index: usize = 0,
        dictionaries: Dictionaries,

        pub fn init(allocator: Allocator, source: ReaderType) !Self {
            return .{
                .allocator = allocator,
                .arena = std.heap.ArenaAllocator.init(allocator),
                .source = source,
                .dictionaries = Dictionaries.init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.dictionaries.deinit();
            self.arena.deinit();
        }

        fn readMessageLen(self: *Self) !usize {
            // > This component was introduced in version 0.15.0 in part to address the 8-byte alignment requirement of Flatbuffers
            var res = try self.source.readIntLittle(shared.MessageLen);
            while (res == shared.continuation) {
                res = try self.source.readIntLittle(shared.MessageLen);
            }

            return @intCast(res);
        }

        pub fn readMessage(self: *Self) !?Message {
            // <continuation: 0xFFFFFFFF> (optional)
            // <metadata_size: int32> (must be multiple of 8)
            // <metadata_flatbuffer: bytes>
            // <message body>
            const message_len = try self.readMessageLen();
            if (message_len == 0) return null; // EOS

            const allocator = self.arena.allocator();
            var message_buf = try allocator.alloc(u8, message_len);
            errdefer allocator.free(message_buf);
            const n_read = try self.source.readAll(message_buf);
            if (n_read != message_buf.len) {
                log.err("expected {d} bytes in message, got {d}", .{ message_buf.len, n_read });
                return IpcError.InvalidLen;
            }

            if (n_read % 8 != 0) {
                log.err("expected message to be padded to 8 bytes, got {d}", .{n_read});
                return IpcError.InvalidLen;
            }

            const packed_message = try flat.PackedMessage.init(message_buf);
            log.debug("read {s} message len {d}", .{
                switch (try packed_message.headerType()) {
                    .none => "none",
                    .schema => "schema",
                    .dictionary_batch => "dictionary_batch",
                    .record_batch => "record_batch",
                },
                message_len,
            });

            return try Message.init(allocator, packed_message);
        }

        pub fn readSchema(self: *Self) !void {
            if (try self.readMessage()) |message| {
                switch (message.header) {
                    .schema => |s| {
                        self.schema = s;
                        self.n_fields = s.nFields();
                        self.n_buffers = try s.nBuffers();
                    },
                    else => {
                        log.err("expected schema message, got {any}", .{message.header});
                        return IpcError.InvalidSchemaMesssage;
                    },
                }
            } else {
                return IpcError.MissingSchemaMessage;
            }
        }

        fn readField(
            self: *Self,
            buffers: []Array.Buffer,
            batch: RecordBatch,
            field: flat.Field,
        ) !*Array {
            const allocator = self.allocator;
            const tag = try field.toMaybeDictTag();
            // log.debug("read field \"{s}\" type {any} n_children {d}", .{ field.name, tag, field.children.items.len });

            const node = batch.nodes[self.node_index];
            var res = try allocator.create(Array);
            errdefer allocator.destroy(res);
            res.* = .{
                .tag = tag,
                .name = field.name,
                .allocator = allocator,
                .length = @bitCast(node.length),
                .null_count = @bitCast(node.null_count),
                .children = try allocator.alloc(*Array, field.children.len),
            };
            errdefer allocator.free(res.children);
            self.node_index += 1;

            for (0..res.tag.abiLayout().nBuffers()) |i| {
                res.buffers[i] = buffers[self.buffer_index];
                self.buffer_index += 1;
            }

            var child_i: usize = 0;
            errdefer {
                for (res.children[0..child_i]) |c| c.deinitAdvanced(true, false);
            }
            for (field.children) |child| {
                res.children[child_i] = try self.readField(buffers, batch, child);
                child_i += 1;
            }
            if (field.dictionary) |d| {
                // log.debug("read field \"{s}\" dictionary {d}", .{ field.name, d.id });
                if (self.dictionaries.get(d.id)) |v| {
                    // TODO: refcount dictionary instead of copying out
                    var dict_values = try allocator.create(Array);
                    errdefer allocator.destroy(dict_values);
                    dict_values.* = .{
                        .tag = try field.toTag(),
                        .name = "dict values",
                        .allocator = allocator,
                        .length = v.length,
                        .null_count = v.null_count,
                    };
                    errdefer for (dict_values.buffers) |b| if (b.len > 0) allocator.free(b);
                    for (0..3) |i| {
                        var src = v.buffers[i].items;
                        if (src.len == 0) continue;
                        dict_values.buffers[i] = try allocator.alignedAlloc(u8, Array.buffer_alignment, src.len);
                        @memcpy(dict_values.buffers[i], src);
                    }
                    res.children = try allocator.alloc(*Array, 1);
                    errdefer allocator.free(res.children);
                    res.children[0] = dict_values;
                } else {
                    log.err("dictionary {d} not previously read", .{d.id});
                    return IpcError.DictNotFound;
                }
            }
            return res;
        }

        fn readBuffer(
            self: *Self,
            allocator: Allocator,
            size: usize,
            compression: ?flat.BodyCompression,
        ) !Array.Buffer {
            // Undocumented, but whatever :)
            if (size == 0) return try allocator.alignedAlloc(u8, Array.buffer_alignment, 0);
            // > Each constituent buffer is first compressed with the indicated
            // > compressor, and then written with the uncompressed length in the first 8
            // > bytes as a 64-bit little-endian signed integer followed by the compressed
            // > buffer bytes (and then padding as required by the protocol). The
            // > uncompressed length may be set to -1 to indicate that the data that
            // > follows is not compressed, which can be useful for cases where
            // > compression does not yield appreciable savings.
            const uncompressed_size = if (compression != null) brk: {
                const res: usize = @bitCast(try self.source.readIntLittle(i64));
                break :brk if (res == -1) size else res;
            } else size;
            var res = try allocator.alignedAlloc(u8, Array.buffer_alignment, uncompressed_size);
            errdefer allocator.free(res);
            const n_read: usize = if (compression) |c| brk: {
                switch (c.codec) {
                    .lz4__frame => {
                        var stream = lz4.decompressStream(self.arena.allocator(), self.source, true);
                        defer stream.deinit();
                        break :brk try stream.reader().readAll(res);
                    },
                    .zstd => {
                        var stream = std.compress.zstd.decompressStream(self.arena.allocator(), self.source);
                        defer stream.deinit();
                        break :brk try stream.reader().readAll(res);
                    },
                }
            } else try self.source.readAll(res); // TODO: check if properly aligned and zero-copy

            if (res.len != n_read) {
                log.err("expected {d} bytes in record batch body, got {any}", .{ res.len, n_read });
                return IpcError.InvalidRecordBatch;
            }
            return res;
        }

        /// Reads flattened buffers
        fn readBuffers(
            self: *Self,
            allocator: Allocator,
            batch: RecordBatch,
            body_len: i64,
        ) ![]Array.Buffer {
            var i: usize = 0;
            var res = try allocator.alloc(Array.Buffer, batch.buffers.len);
            errdefer {
                for (0..i) |j| allocator.free(res[j]);
                allocator.free(res);
            }

            for (batch.buffers) |info| {
                const size: usize = @bitCast(info.length);
                res[i] = try self.readBuffer(allocator, size, batch.compression);

                const next_offset = if (i == res.len - 1)
                    body_len
                else
                    batch.buffers[i + 1].offset;
                i += 1;

                const seek: u64 = @intCast(next_offset - (info.offset + info.length));
                try self.source.skipBytes(seek, .{});
            }

            return res;
        }

        /// Reads record batch. Must have already read a schema. Caller owns returned array.
        pub fn readBatch(self: *Self, message: *const Message) !?*Array {
            return switch (message.header) {
                .record_batch => |batch| {
                    if (self.schema == null) return IpcError.NoSchema;
                    // https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message
                    // > Fields and buffers are flattened by a pre-order depth-first traversal of the fields in the
                    // > record batch.
                    log.debug("read batch len {d} compression {s}", .{
                        batch.length,
                        if (batch.compression) |c| @tagName(c.codec) else "none",
                    });

                    const allocator = self.allocator;

                    // Quickly check that the number of buffers and field nodes matches the schema.
                    const n_buffers = batch.buffers.len;
                    if (n_buffers != self.n_buffers) {
                        log.warn("skipped reading batch with {d} buffers (schema expects {d})", .{
                            n_buffers,
                            self.n_buffers,
                        });
                        return IpcError.InvalidLen;
                    }
                    const n_nodes = batch.nodes.len;
                    if (n_nodes != self.n_fields) {
                        log.warn("skipped reading batch with {d} fields (schema expects {d})", .{
                            n_nodes,
                            self.n_fields,
                        });
                        return IpcError.InvalidLen;
                    }

                    const buffers = try self.readBuffers(allocator, batch, message.body_length);
                    defer allocator.free(buffers);
                    errdefer for (buffers) |b| allocator.free(b);

                    // Recursively read tags, name, and buffers into arrays from `schema.fields`
                    self.node_index = 0;
                    self.buffer_index = 0;
                    var children = try allocator.alloc(*Array, self.schema.?.fields.len);
                    errdefer allocator.free(children);
                    var child_i: usize = 0;
                    errdefer for (children[0..child_i]) |c| c.deinitAdvanced(true, false);
                    for (self.schema.?.fields) |field| {
                        children[child_i] = try self.readField(buffers, batch, field);
                        child_i += 1;
                    }

                    const res = try allocator.create(Array);
                    res.* = .{
                        .tag = .{ .Struct = .{ .nullable = false } },
                        .name = "record batch",
                        .allocator = allocator,
                        .length = @bitCast(batch.length),
                        .null_count = 0,
                        .children = children,
                    };
                    return res;
                },
                else => IpcError.InvalidRecordBatchHeader,
            };
        }

        /// Reads dictionary batch into memory for later use in record batches.
        pub fn readDict(self: *Self, message: *const Message) !void {
            return switch (message.header) {
                .dictionary_batch => |dict| {
                    // We own the dictionaries due to any message being able to update them. The values are
                    // copied out into arrays.
                    // An alternative is adding reference counting support to Array.
                    const allocator = self.arena.allocator();
                    const id = dict.id;

                    log.debug("read dict id {d}", .{id});
                    if (dict.data == null) return IpcError.NoDictionaryData;
                    const batch: RecordBatch = dict.data.?;
                    const n_actual: usize = batch.buffers.len;
                    if (n_actual > 3) {
                        log.warn("expected dictionary data to have 3 or fewer buffers, got {d}", .{n_actual});
                        return IpcError.InvalidNumDictionaryBuffers;
                    }

                    const buffers = try self.readBuffers(allocator, batch, message.body_length);
                    defer allocator.free(buffers);

                    if (dict.is_delta) {
                        if (self.dictionaries.getPtr(id)) |existing| {
                            for (0..buffers.len) |i| try existing.buffers[i].appendSlice(buffers[i]);
                        } else {
                            log.warn("ignoring delta for non-existant dictionary {d}", .{id});
                        }
                    } else {
                        var mutable_bufs: [3]std.ArrayList(u8) = undefined;
                        for (&mutable_bufs, buffers) |*dest, src| {
                            dest.* = try std.ArrayList(u8).initCapacity(allocator, src.len);
                            try dest.appendSlice(src);
                        }

                        if (batch.nodes.len != 1) {
                            log.warn("expected dictionary to have exactly 1 node, got {d}", .{batch.nodes.len});
                            return IpcError.InvalidNumDictionaryNodes;
                        }
                        const node = batch.nodes[0];

                        if (try self.dictionaries.fetchPut(id, .{
                            .length = @bitCast(node.length),
                            .null_count = @bitCast(node.null_count),
                            .buffers = mutable_bufs,
                        })) |existing| {
                            log.warn("spec does not support replacing dictionary id {d}, doing it anyways :)", .{id});
                            existing.value.deinit();
                        }
                    }
                },
                else => IpcError.InvalidDictionaryBatchHeader,
            };
        }

        /// Reads until a record batch and returns it. Caller owns Array.
        pub fn nextBatch(self: *Self) !?*Array {
            if (try self.readMessage()) |message| {
                switch (message.header) {
                    .none => {
                        log.warn("ignoring unexpected message {any}", .{message.HeaderType()});
                        try self.source.skipBytes(message.BodyLength(), .{});
                    },
                    .schema => {
                        try self.readSchema();
                        // Keep going until a record batch
                        return self.nextBatch();
                    },
                    .dictionary_batch => {
                        try self.readDict(&message);
                        // Keep going until a record batch
                        return self.nextBatch();
                    },
                    .record_batch => return try self.readBatch(&message),
                }
            }
            return null;
        }
    };
}

pub fn reader(allocator: Allocator, reader_: anytype) !Reader(@TypeOf(reader_)) {
    return Reader(@TypeOf(reader_)).init(allocator, reader_);
}

/// Handles file header, footer and strange dictionary requirements. Convienently closes file in .deinit.
const FileReader = struct {
    const Self = @This();
    const ReaderType = Reader(std.fs.File.Reader);

    allocator: Allocator,
    file: std.fs.File,
    footer: Footer,
    reader: ReaderType,
    batch_index: usize,

    fn readMagic(file: std.fs.File, comptime is_start: bool) !void {
        var maybe_magic: [shared.magic.len]u8 = undefined;
        const n_read = try file.readAll(&maybe_magic);
        const location = if (is_start) "start" else "end";
        if (shared.magic.len != n_read) {
            log.err("expected {s} magic len {d}, got {d}", .{ location, shared.magic.len, n_read });
            return IpcError.InvalidMagicLen;
        }
        if (!std.mem.eql(u8, shared.magic, &maybe_magic)) {
            log.err("expected {s} magic {s}, got {s}", .{ location, shared.magic, maybe_magic });
            return IpcError.InvalidMagic;
        }
    }

    fn readFooter(allocator: Allocator, file: std.fs.File) !Footer {
        const FooterSize = i32;
        const seek_back = -(@as(i64, shared.magic.len) + @sizeOf(FooterSize));
        try file.seekFromEnd(seek_back);
        const pos = try file.getPos();
        const footer_len = try file.reader().readIntLittle(FooterSize);
        if (footer_len <= 0 or footer_len > pos) {
            log.err("invalid footer len {d}. it's <= 0 or > {d}", .{ footer_len, pos });
            return IpcError.InvalidFooterLen;
        }
        try readMagic(file, false);

        const footer_buf = try allocator.alloc(u8, @as(usize, @intCast(footer_len)));
        defer allocator.free(footer_buf);
        try file.seekFromEnd(seek_back - footer_len);
        const n_read = try file.readAll(footer_buf);
        if (n_read != footer_buf.len) return IpcError.InvalidLen;

        const packed_footer = try flat.PackedFooter.init(footer_buf);
        return try Footer.init(allocator, packed_footer);
    }

    fn initReader(allocator: Allocator, file: std.fs.File, footer: Footer) !ReaderType {
        try file.seekTo(8);
        var reader_ = try reader(allocator, file.reader());
        errdefer reader_.deinit();

        // Read the schema from the streaming format.
        // TODO: validate footer matches
        try reader_.readSchema();

        // From the spec:
        // > In the file format, there is no requirement that dictionary keys should be defined in
        // > a DictionaryBatch before they are used in a RecordBatch, as long as the keys are defined
        // > somewhere in the file.
        // We respect this by reading all dictionaries on init.

        // > Further more, it is invalid to have more than one non-delta dictionary batch per
        // > dictionary ID (i.e. dictionary replacement is not supported).
        // We disrespect this by reading them all anyways. If one gets replaced we warn the user
        // that it's against the spec.

        // > Delta dictionaries are applied in the order they appear in the file footer.
        // We respect this.

        for (footer.dictionaries) |d| {
            const offset: usize = @bitCast(d.offset);
            try file.seekTo(offset);
            if (try reader_.readMessage()) |message| {
                try reader_.readDict(&message);
            } else {
                log.warn("missing dictionary message at offset {d}", .{offset});
            }
        }

        return reader_;
    }

    pub fn init(allocator: Allocator, fname: []const u8) !Self {
        var file = try std.fs.cwd().openFile(fname, .{});

        try readMagic(file, true);

        const footer = try readFooter(allocator, file);
        log.debug("read footer {any} {any}", .{ footer.dictionaries, footer.record_batches });
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
        self.footer.deinit(self.allocator);
        self.reader.deinit();
        self.file.close();
    }

    pub fn nextBatch(self: *Self) !?*Array {
        if (self.batch_index >= self.footer.record_batches.len) return null;

        const rb = self.footer.record_batches[self.batch_index];
        const offset: usize = @bitCast(rb.offset);
        try self.file.seekTo(offset);
        self.batch_index += 1;

        if (try self.reader.readMessage()) |message| {
            errdefer message.deinit(self.reader.arena.allocator());
            return try self.reader.readBatch(&message);
        }
        log.err("expected non-empty record batch message at offset {d}", .{offset});
        return IpcError.InvalidRecordBatch;
    }
};

pub fn fileReader(allocator: Allocator, fname: []const u8) !FileReader {
    return FileReader.init(allocator, fname);
}

const sample = @import("../sample.zig");

pub fn testEquals(arr1: *Array, arr2: *Array) !void {
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

fn testSample(fname: []const u8) !void {
    var ipc_reader = try fileReader(std.testing.allocator, fname);
    defer ipc_reader.deinit();

    const expected = try sample.all(std.testing.allocator);
    try expected.toRecordBatch("record batch");
    defer expected.deinit();
    try std.testing.expectEqual(expected.children.len, ipc_reader.reader.schema.?.fields.len);

    var n_batches: usize = 0;
    while (try ipc_reader.nextBatch()) |rb| {
        defer rb.deinit();
        try testEquals(expected, rb);
        n_batches += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), n_batches);
}

test "read uncompressed sample file" {
    try testSample("./testdata/sample.arrow");
}

test "read lz4 compressed sample file" {
    try testSample("./testdata/sample.lz4.arrow");
}

test "read zstd compressed sample file" {
    try testSample("./testdata/sample.zstd.arrow");
}

test "read zstd compressed tickers file with multiple record batches" {
    var ipc_reader = try fileReader(std.testing.allocator, "./testdata/tickers.arrow");
    defer ipc_reader.deinit();

    var n_batches: usize = 0;
    while (try ipc_reader.nextBatch()) |rb| {
        defer rb.deinit();
        n_batches += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), n_batches);
}
