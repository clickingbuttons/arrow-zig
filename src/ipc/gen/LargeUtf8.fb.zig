//!
//! generated by flatc-zig
//! binary:     gen/format/Schema.bfbs
//! schema:     format/Schema.fbs
//! file ident: //Schema.fbs
//! typename    LargeUtf8
//!

const std = @import("std");
const fb = @import("flatbufferz");
const Builder = fb.Builder;

/// Same as Utf8, but with 64-bit offsets, allowing to represent
/// extremely large data values.
pub const LargeUtf8T = struct {
    pub fn Pack(rcv: LargeUtf8T, __builder: *Builder, __pack_opts: fb.common.PackOptions) fb.common.PackError!u32 {
        _ = .{__pack_opts};
        var __tmp_offsets = std.ArrayListUnmanaged(u32){};
        defer if (__pack_opts.allocator) |alloc| __tmp_offsets.deinit(alloc);
        _ = rcv;
        try LargeUtf8.Start(__builder);
        return LargeUtf8.End(__builder);
    }

    pub fn UnpackTo(rcv: LargeUtf8, t: *LargeUtf8T, __pack_opts: fb.common.PackOptions) !void {
        _ = .{__pack_opts};
        _ = rcv;
        _ = t;
    }

    pub fn Unpack(rcv: LargeUtf8, __pack_opts: fb.common.PackOptions) fb.common.PackError!LargeUtf8T {
        var t = LargeUtf8T{};
        try LargeUtf8T.UnpackTo(rcv, &t, __pack_opts);
        return t;
    }

    pub fn deinit(self: *LargeUtf8T, allocator: std.mem.Allocator) void {
        _ = .{ self, allocator };
    }
};

pub const LargeUtf8 = struct {
    _tab: fb.Table,

    pub fn GetRootAs(buf: []u8, offset: u32) LargeUtf8 {
        const n = fb.encode.read(u32, buf[offset..]);
        return LargeUtf8.init(buf, n + offset);
    }

    pub fn GetSizePrefixedRootAs(buf: []u8, offset: u32) LargeUtf8 {
        const n = fb.encode.read(u32, buf[offset + fb.Builder.size_u32 ..]);
        return LargeUtf8.init(buf, n + offset + fb.Builder.size_u32);
    }

    pub fn init(bytes: []u8, pos: u32) LargeUtf8 {
        return .{ ._tab = .{ .bytes = bytes, .pos = pos } };
    }

    pub fn Table(x: LargeUtf8) fb.Table {
        return x._tab;
    }

    pub fn Start(__builder: *Builder) !void {
        try __builder.startObject(0);
    }
    pub fn End(__builder: *Builder) !u32 {
        return __builder.endObject();
    }

    pub fn Unpack(rcv: LargeUtf8, __pack_opts: fb.common.PackOptions) !LargeUtf8T {
        return LargeUtf8T.Unpack(rcv, __pack_opts);
    }
    pub fn FinishBuffer(__builder: *Builder, root: u32) !void {
        return __builder.Finish(root);
    }

    pub fn FinishSizePrefixedBuffer(__builder: *Builder, root: u32) !void {
        return __builder.FinishSizePrefixed(root);
    }
};
