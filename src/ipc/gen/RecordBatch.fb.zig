//!
//! generated by flatc-zig
//! binary:     gen/format/Message.bfbs
//! schema:     format/Message.fbs
//! file ident: //Message.fbs
//! typename    RecordBatch
//!

const std = @import("std");
const fb = @import("flatbufferz");
const Builder = fb.Builder;

const Buffer = @import("Buffer.fb.zig").Buffer;
const BufferT = @import("Buffer.fb.zig").BufferT;
const BodyCompression = @import("BodyCompression.fb.zig").BodyCompression;
const BodyCompressionT = @import("BodyCompression.fb.zig").BodyCompressionT;
const FieldNode = @import("FieldNode.fb.zig").FieldNode;
const FieldNodeT = @import("FieldNode.fb.zig").FieldNodeT;

/// A data header describing the shared memory layout of a "record" or "row"
/// batch. Some systems call this a "row batch" internally and others a "record
/// batch".
pub const RecordBatchT = struct {
    /// number of records / rows. The arrays in the batch should all have this
    /// length
    length: i64 = 0,
    /// Nodes correspond to the pre-ordered flattened logical schema
    nodes: std.ArrayListUnmanaged(FieldNodeT) = .{},
    /// Buffers correspond to the pre-ordered flattened buffer tree
    ///
    /// The number of buffers appended to this list depends on the schema. For
    /// example, most primitive arrays will have 2 buffers, 1 for the validity
    /// bitmap and 1 for the values. For struct arrays, there will only be a
    /// single buffer for the validity (nulls) bitmap
    buffers: std.ArrayListUnmanaged(BufferT) = .{},
    /// Optional compression of the message body
    compression: ?*BodyCompressionT = null,

    pub fn Pack(rcv: RecordBatchT, __builder: *Builder, __pack_opts: fb.common.PackOptions) fb.common.PackError!u32 {
        _ = .{__pack_opts};
        var __tmp_offsets = std.ArrayListUnmanaged(u32){};
        defer if (__pack_opts.allocator) |alloc| __tmp_offsets.deinit(alloc);
        var nodes_off: u32 = 0;
        if (rcv.nodes.items.len != 0) {
            const nodes_len = @intCast(i32, rcv.nodes.items.len);
            _ = try RecordBatch.StartNodesVector(__builder, nodes_len);
            {
                var j = nodes_len - 1;
                while (j >= 0) : (j -= 1) {
                    _ = try rcv.nodes.items[@bitCast(u32, j)].Pack(__builder, __pack_opts);
                }
                nodes_off = try __builder.endVector(@bitCast(u32, nodes_len));
            }
        }

        var buffers_off: u32 = 0;
        if (rcv.buffers.items.len != 0) {
            const buffers_len = @intCast(i32, rcv.buffers.items.len);
            _ = try RecordBatch.StartBuffersVector(__builder, buffers_len);
            {
                var j = buffers_len - 1;
                while (j >= 0) : (j -= 1) {
                    _ = try rcv.buffers.items[@bitCast(u32, j)].Pack(__builder, __pack_opts);
                }
                buffers_off = try __builder.endVector(@bitCast(u32, buffers_len));
            }
        }

        const compression_off = if (rcv.compression) |x| try x.Pack(__builder, __pack_opts) else 0;

        try RecordBatch.Start(__builder);
        try RecordBatch.AddLength(__builder, rcv.length);
        try RecordBatch.AddNodes(__builder, nodes_off);
        try RecordBatch.AddBuffers(__builder, buffers_off);
        try RecordBatch.AddCompression(__builder, compression_off);
        return RecordBatch.End(__builder);
    }

    pub fn UnpackTo(rcv: RecordBatch, t: *RecordBatchT, __pack_opts: fb.common.PackOptions) !void {
        _ = .{__pack_opts};
        t.length = rcv.Length();

        const nodes_len = rcv.NodesLen();
        t.nodes = try std.ArrayListUnmanaged(FieldNodeT).initCapacity(__pack_opts.allocator.?, @bitCast(u32, nodes_len));
        t.nodes.expandToCapacity();
        {
            var j: u32 = 0;
            while (j < nodes_len) : (j += 1) {
                const x = rcv.Nodes(j).?;
                t.nodes.items[j] = try x.Unpack(__pack_opts);
            }
        }

        const buffers_len = rcv.BuffersLen();
        t.buffers = try std.ArrayListUnmanaged(BufferT).initCapacity(__pack_opts.allocator.?, @bitCast(u32, buffers_len));
        t.buffers.expandToCapacity();
        {
            var j: u32 = 0;
            while (j < buffers_len) : (j += 1) {
                const x = rcv.Buffers(j).?;
                t.buffers.items[j] = try x.Unpack(__pack_opts);
            }
        }

        if (rcv.Compression()) |x| {
            if (t.compression == null) {
                t.compression = try __pack_opts.allocator.?.create(BodyCompressionT);
                t.compression.?.* = .{};
            }
            try BodyCompressionT.UnpackTo(x, t.compression.?, __pack_opts);
        }
    }

    pub fn Unpack(rcv: RecordBatch, __pack_opts: fb.common.PackOptions) fb.common.PackError!RecordBatchT {
        var t = RecordBatchT{};
        try RecordBatchT.UnpackTo(rcv, &t, __pack_opts);
        return t;
    }

    pub fn deinit(self: *RecordBatchT, allocator: std.mem.Allocator) void {
        _ = .{ self, allocator };
        for (self.nodes.items) |*it| it.deinit(allocator);
        self.nodes.deinit(allocator);
        for (self.buffers.items) |*it| it.deinit(allocator);
        self.buffers.deinit(allocator);
        if (self.compression) |x| {
            x.deinit(allocator);
            allocator.destroy(x);
        }
    }
};

pub const RecordBatch = struct {
    _tab: fb.Table,

    pub fn GetRootAs(buf: []u8, offset: u32) RecordBatch {
        const n = fb.encode.read(u32, buf[offset..]);
        return RecordBatch.init(buf, n + offset);
    }

    pub fn GetSizePrefixedRootAs(buf: []u8, offset: u32) RecordBatch {
        const n = fb.encode.read(u32, buf[offset + fb.Builder.size_u32 ..]);
        return RecordBatch.init(buf, n + offset + fb.Builder.size_u32);
    }

    pub fn init(bytes: []u8, pos: u32) RecordBatch {
        return .{ ._tab = .{ .bytes = bytes, .pos = pos } };
    }

    pub fn Table(x: RecordBatch) fb.Table {
        return x._tab;
    }

    /// number of records / rows. The arrays in the batch should all have this
    /// length
    pub fn Length(rcv: RecordBatch) i64 {
        const o = rcv._tab.offset(4);
        if (o != 0) {
            return rcv._tab.read(i64, o + rcv._tab.pos);
        }
        return 0;
    }

    pub fn MutateLength(rcv: RecordBatch, n: i64) bool {
        return rcv._tab.mutateSlot(i64, 4, n);
    }

    /// Nodes correspond to the pre-ordered flattened logical schema
    pub fn Nodes(rcv: RecordBatch, j: usize) ?FieldNode {
        const o = rcv._tab.offset(6);
        if (o != 0) {
            var x = rcv._tab.vector(o);
            x += @intCast(u32, j) * 16;
            return FieldNode.init(rcv._tab.bytes, x);
        }
        return null;
    }

    pub fn NodesLen(rcv: RecordBatch) u32 {
        const o = rcv._tab.offset(6);
        if (o != 0) {
            return rcv._tab.vectorLen(o);
        }
        return 0;
    }

    /// Buffers correspond to the pre-ordered flattened buffer tree
    ///
    /// The number of buffers appended to this list depends on the schema. For
    /// example, most primitive arrays will have 2 buffers, 1 for the validity
    /// bitmap and 1 for the values. For struct arrays, there will only be a
    /// single buffer for the validity (nulls) bitmap
    pub fn Buffers(rcv: RecordBatch, j: usize) ?Buffer {
        const o = rcv._tab.offset(8);
        if (o != 0) {
            var x = rcv._tab.vector(o);
            x += @intCast(u32, j) * 16;
            return Buffer.init(rcv._tab.bytes, x);
        }
        return null;
    }

    pub fn BuffersLen(rcv: RecordBatch) u32 {
        const o = rcv._tab.offset(8);
        if (o != 0) {
            return rcv._tab.vectorLen(o);
        }
        return 0;
    }

    /// Optional compression of the message body
    pub fn Compression(rcv: RecordBatch) ?BodyCompression {
        const o = rcv._tab.offset(10);
        if (o != 0) {
            const x = rcv._tab.indirect(o + rcv._tab.pos);
            return BodyCompression.init(rcv._tab.bytes, x);
        }
        return null;
    }

    pub fn Start(__builder: *Builder) !void {
        try __builder.startObject(4);
    }
    pub fn AddLength(__builder: *Builder, length: i64) !void {
        try __builder.prependSlot(i64, 0, length, 0);
    }

    pub fn AddNodes(__builder: *Builder, nodes: u32) !void {
        try __builder.prependSlotUOff(1, nodes, 0);
    }

    pub fn StartNodesVector(__builder: *Builder, num_elems: i32) !u32 {
        return __builder.startVector(16, num_elems, 8);
    }
    pub fn AddBuffers(__builder: *Builder, buffers: u32) !void {
        try __builder.prependSlotUOff(2, buffers, 0);
    }

    pub fn StartBuffersVector(__builder: *Builder, num_elems: i32) !u32 {
        return __builder.startVector(16, num_elems, 8);
    }
    pub fn AddCompression(__builder: *Builder, compression: u32) !void {
        try __builder.prependSlotUOff(3, compression, 0);
    }

    pub fn End(__builder: *Builder) !u32 {
        return __builder.endObject();
    }

    pub fn Unpack(rcv: RecordBatch, __pack_opts: fb.common.PackOptions) !RecordBatchT {
        return RecordBatchT.Unpack(rcv, __pack_opts);
    }
    pub fn FinishBuffer(__builder: *Builder, root: u32) !void {
        return __builder.Finish(root);
    }

    pub fn FinishSizePrefixedBuffer(__builder: *Builder, root: u32) !void {
        return __builder.FinishSizePrefixed(root);
    }
};