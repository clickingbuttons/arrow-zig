//!
//! generated by flatc-zig
//! binary:     src/gen/format/Schema.bfbs
//! schema:     format/Schema.fbs
//! file ident: //Schema.fbs
//! typename    LargeList
//!

const std = @import("std");
const fb = @import("flatbufferz");
const Builder = fb.Builder;

/// Same as List, but with 64-bit offsets, allowing to represent
/// extremely large data values.
pub const LargeListT = struct {
    pub fn Pack(rcv: LargeListT, __builder: *Builder, __pack_opts: fb.common.PackOptions) fb.common.PackError!u32 {
        _ = .{__pack_opts};
        var __tmp_offsets = std.ArrayListUnmanaged(u32){};
        defer if (__pack_opts.allocator) |alloc| __tmp_offsets.deinit(alloc);
        _ = rcv;
        try LargeList.Start(__builder);
        return LargeList.End(__builder);
    }

    pub fn UnpackTo(rcv: LargeList, t: *LargeListT, __pack_opts: fb.common.PackOptions) !void {
        _ = .{__pack_opts};
        _ = rcv;
        _ = t;
    }

    pub fn Unpack(rcv: LargeList, __pack_opts: fb.common.PackOptions) fb.common.PackError!LargeListT {
        var t = LargeListT{};
        try LargeListT.UnpackTo(rcv, &t, __pack_opts);
        return t;
    }

    pub fn deinit(self: *LargeListT, allocator: std.mem.Allocator) void {
        _ = .{ self, allocator };
    }
};

pub const LargeList = struct {
    _tab: fb.Table,

    pub fn GetRootAs(buf: []u8, offset: u32) LargeList {
        const n = fb.encode.read(u32, buf[offset..]);
        return LargeList.init(buf, n + offset);
    }

    pub fn GetSizePrefixedRootAs(buf: []u8, offset: u32) LargeList {
        const n = fb.encode.read(u32, buf[offset + fb.Builder.size_u32 ..]);
        return LargeList.init(buf, n + offset + fb.Builder.size_u32);
    }

    pub fn init(bytes: []u8, pos: u32) LargeList {
        return .{ ._tab = .{ .bytes = bytes, .pos = pos } };
    }

    pub fn Table(x: LargeList) fb.Table {
        return x._tab;
    }

    pub fn Start(__builder: *Builder) !void {
        try __builder.startObject(0);
    }
    pub fn End(__builder: *Builder) !u32 {
        return __builder.endObject();
    }

    pub fn Unpack(rcv: LargeList, __pack_opts: fb.common.PackOptions) !LargeListT {
        return LargeListT.Unpack(rcv, __pack_opts);
    }
    pub fn FinishBuffer(__builder: *Builder, root: u32) !void {
        return __builder.Finish(root);
    }

    pub fn FinishSizePrefixedBuffer(__builder: *Builder, root: u32) !void {
        return __builder.FinishSizePrefixed(root);
    }
};