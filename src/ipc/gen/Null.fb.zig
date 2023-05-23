//!
//! generated by flatc-zig
//! binary:     gen/format/Schema.bfbs
//! schema:     format/Schema.fbs
//! file ident: //Schema.fbs
//! typename    Null
//!

const std = @import("std");
const fb = @import("flatbufferz");
const Builder = fb.Builder;

/// These are stored in the flatbuffer in the Type union below
pub const NullT = struct {
    pub fn Pack(rcv: NullT, __builder: *Builder, __pack_opts: fb.common.PackOptions) fb.common.PackError!u32 {
        _ = .{__pack_opts};
        var __tmp_offsets = std.ArrayListUnmanaged(u32){};
        defer if (__pack_opts.allocator) |alloc| __tmp_offsets.deinit(alloc);
        _ = rcv;
        try Null.Start(__builder);
        return Null.End(__builder);
    }

    pub fn UnpackTo(rcv: Null, t: *NullT, __pack_opts: fb.common.PackOptions) !void {
        _ = .{__pack_opts};
        _ = rcv;
        _ = t;
    }

    pub fn Unpack(rcv: Null, __pack_opts: fb.common.PackOptions) fb.common.PackError!NullT {
        var t = NullT{};
        try NullT.UnpackTo(rcv, &t, __pack_opts);
        return t;
    }

    pub fn deinit(self: *NullT, allocator: std.mem.Allocator) void {
        _ = .{ self, allocator };
    }
};

pub const Null = struct {
    _tab: fb.Table,

    pub fn GetRootAs(buf: []u8, offset: u32) Null {
        const n = fb.encode.read(u32, buf[offset..]);
        return Null.init(buf, n + offset);
    }

    pub fn GetSizePrefixedRootAs(buf: []u8, offset: u32) Null {
        const n = fb.encode.read(u32, buf[offset + fb.Builder.size_u32 ..]);
        return Null.init(buf, n + offset + fb.Builder.size_u32);
    }

    pub fn init(bytes: []u8, pos: u32) Null {
        return .{ ._tab = .{ .bytes = bytes, .pos = pos } };
    }

    pub fn Table(x: Null) fb.Table {
        return x._tab;
    }

    pub fn Start(__builder: *Builder) !void {
        try __builder.startObject(0);
    }
    pub fn End(__builder: *Builder) !u32 {
        return __builder.endObject();
    }

    pub fn Unpack(rcv: Null, __pack_opts: fb.common.PackOptions) !NullT {
        return NullT.Unpack(rcv, __pack_opts);
    }
    pub fn FinishBuffer(__builder: *Builder, root: u32) !void {
        return __builder.Finish(root);
    }

    pub fn FinishSizePrefixedBuffer(__builder: *Builder, root: u32) !void {
        return __builder.FinishSizePrefixed(root);
    }
};