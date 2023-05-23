//!
//! generated by flatc-zig
//! binary:     gen/format/Schema.bfbs
//! schema:     format/Schema.fbs
//! file ident: //Schema.fbs
//! typename    FloatingPoint
//!

const std = @import("std");
const fb = @import("flatbufferz");
const Builder = fb.Builder;

const Precision = @import("Precision.fb.zig").Precision;

pub const FloatingPointT = struct {
    precision: Precision = @intToEnum(Precision, 0),

    pub fn Pack(rcv: FloatingPointT, __builder: *Builder, __pack_opts: fb.common.PackOptions) fb.common.PackError!u32 {
        _ = .{__pack_opts};
        var __tmp_offsets = std.ArrayListUnmanaged(u32){};
        defer if (__pack_opts.allocator) |alloc| __tmp_offsets.deinit(alloc);
        try FloatingPoint.Start(__builder);
        try FloatingPoint.AddPrecision_(__builder, rcv.precision);
        return FloatingPoint.End(__builder);
    }

    pub fn UnpackTo(rcv: FloatingPoint, t: *FloatingPointT, __pack_opts: fb.common.PackOptions) !void {
        _ = .{__pack_opts};
        t.precision = rcv.Precision_();
    }

    pub fn Unpack(rcv: FloatingPoint, __pack_opts: fb.common.PackOptions) fb.common.PackError!FloatingPointT {
        var t = FloatingPointT{};
        try FloatingPointT.UnpackTo(rcv, &t, __pack_opts);
        return t;
    }

    pub fn deinit(self: *FloatingPointT, allocator: std.mem.Allocator) void {
        _ = .{ self, allocator };
    }
};

pub const FloatingPoint = struct {
    _tab: fb.Table,

    pub fn GetRootAs(buf: []u8, offset: u32) FloatingPoint {
        const n = fb.encode.read(u32, buf[offset..]);
        return FloatingPoint.init(buf, n + offset);
    }

    pub fn GetSizePrefixedRootAs(buf: []u8, offset: u32) FloatingPoint {
        const n = fb.encode.read(u32, buf[offset + fb.Builder.size_u32 ..]);
        return FloatingPoint.init(buf, n + offset + fb.Builder.size_u32);
    }

    pub fn init(bytes: []u8, pos: u32) FloatingPoint {
        return .{ ._tab = .{ .bytes = bytes, .pos = pos } };
    }

    pub fn Table(x: FloatingPoint) fb.Table {
        return x._tab;
    }

    pub fn Precision_(rcv: FloatingPoint) Precision {
        const o = rcv._tab.offset(4);
        if (o != 0) {
            return rcv._tab.read(Precision, o + rcv._tab.pos);
        }
        return @intToEnum(Precision, 0);
    }

    pub fn MutatePrecision_(rcv: FloatingPoint, n: Precision) bool {
        return rcv._tab.mutateSlot(Precision, 4, n);
    }

    pub fn Start(__builder: *Builder) !void {
        try __builder.startObject(1);
    }
    pub fn AddPrecision_(__builder: *Builder, precision: Precision) !void {
        try __builder.prependSlot(Precision, 0, precision, @intToEnum(Precision, 0));
    }

    pub fn End(__builder: *Builder) !u32 {
        return __builder.endObject();
    }

    pub fn Unpack(rcv: FloatingPoint, __pack_opts: fb.common.PackOptions) !FloatingPointT {
        return FloatingPointT.Unpack(rcv, __pack_opts);
    }
    pub fn FinishBuffer(__builder: *Builder, root: u32) !void {
        return __builder.Finish(root);
    }

    pub fn FinishSizePrefixedBuffer(__builder: *Builder, root: u32) !void {
        return __builder.FinishSizePrefixed(root);
    }
};