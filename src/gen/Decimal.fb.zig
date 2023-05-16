//!
//! generated by flatc-zig
//! binary:     src/gen/format/Schema.bfbs
//! schema:     format/Schema.fbs
//! file ident: //Schema.fbs
//! typename    Decimal
//!

const std = @import("std");
const fb = @import("flatbufferz");
const Builder = fb.Builder;

/// Exact decimal value represented as an integer value in two's
/// complement. Currently only 128-bit (16-byte) and 256-bit (32-byte) integers
/// are used. The representation uses the endianness indicated
/// in the Schema.
pub const DecimalT = struct {
    /// Total number of decimal digits
    precision: i32 = 0,
    /// Number of digits after the decimal point "."
    scale: i32 = 0,
    /// Number of bits per value. The only accepted widths are 128 and 256.
    /// We use bitWidth for consistency with Int::bitWidth.
    bitWidth: i32 = 128,

    pub fn Pack(rcv: DecimalT, __builder: *Builder, __pack_opts: fb.common.PackOptions) fb.common.PackError!u32 {
        _ = .{__pack_opts};
        var __tmp_offsets = std.ArrayListUnmanaged(u32){};
        defer if (__pack_opts.allocator) |alloc| __tmp_offsets.deinit(alloc);
        try Decimal.Start(__builder);
        try Decimal.AddPrecision(__builder, rcv.precision);
        try Decimal.AddScale(__builder, rcv.scale);
        try Decimal.AddBitWidth(__builder, rcv.bitWidth);
        return Decimal.End(__builder);
    }

    pub fn UnpackTo(rcv: Decimal, t: *DecimalT, __pack_opts: fb.common.PackOptions) !void {
        _ = .{__pack_opts};
        t.precision = rcv.Precision();

        t.scale = rcv.Scale();

        t.bitWidth = rcv.BitWidth();
    }

    pub fn Unpack(rcv: Decimal, __pack_opts: fb.common.PackOptions) fb.common.PackError!DecimalT {
        var t = DecimalT{};
        try DecimalT.UnpackTo(rcv, &t, __pack_opts);
        return t;
    }

    pub fn deinit(self: *DecimalT, allocator: std.mem.Allocator) void {
        _ = .{ self, allocator };
    }
};

pub const Decimal = struct {
    _tab: fb.Table,

    pub fn GetRootAs(buf: []u8, offset: u32) Decimal {
        const n = fb.encode.read(u32, buf[offset..]);
        return Decimal.init(buf, n + offset);
    }

    pub fn GetSizePrefixedRootAs(buf: []u8, offset: u32) Decimal {
        const n = fb.encode.read(u32, buf[offset + fb.Builder.size_u32 ..]);
        return Decimal.init(buf, n + offset + fb.Builder.size_u32);
    }

    pub fn init(bytes: []u8, pos: u32) Decimal {
        return .{ ._tab = .{ .bytes = bytes, .pos = pos } };
    }

    pub fn Table(x: Decimal) fb.Table {
        return x._tab;
    }

    /// Total number of decimal digits
    pub fn Precision(rcv: Decimal) i32 {
        const o = rcv._tab.offset(4);
        if (o != 0) {
            return rcv._tab.read(i32, o + rcv._tab.pos);
        }
        return 0;
    }

    pub fn MutatePrecision(rcv: Decimal, n: i32) bool {
        return rcv._tab.mutateSlot(i32, 4, n);
    }

    /// Number of digits after the decimal point "."
    pub fn Scale(rcv: Decimal) i32 {
        const o = rcv._tab.offset(6);
        if (o != 0) {
            return rcv._tab.read(i32, o + rcv._tab.pos);
        }
        return 0;
    }

    pub fn MutateScale(rcv: Decimal, n: i32) bool {
        return rcv._tab.mutateSlot(i32, 6, n);
    }

    /// Number of bits per value. The only accepted widths are 128 and 256.
    /// We use bitWidth for consistency with Int::bitWidth.
    pub fn BitWidth(rcv: Decimal) i32 {
        const o = rcv._tab.offset(8);
        if (o != 0) {
            return rcv._tab.read(i32, o + rcv._tab.pos);
        }
        return 128;
    }

    pub fn MutateBitWidth(rcv: Decimal, n: i32) bool {
        return rcv._tab.mutateSlot(i32, 8, n);
    }

    pub fn Start(__builder: *Builder) !void {
        try __builder.startObject(3);
    }
    pub fn AddPrecision(__builder: *Builder, precision: i32) !void {
        try __builder.prependSlot(i32, 0, precision, 0);
    }

    pub fn AddScale(__builder: *Builder, scale: i32) !void {
        try __builder.prependSlot(i32, 1, scale, 0);
    }

    pub fn AddBitWidth(__builder: *Builder, bitWidth: i32) !void {
        try __builder.prependSlot(i32, 2, bitWidth, 128);
    }

    pub fn End(__builder: *Builder) !u32 {
        return __builder.endObject();
    }

    pub fn Unpack(rcv: Decimal, __pack_opts: fb.common.PackOptions) !DecimalT {
        return DecimalT.Unpack(rcv, __pack_opts);
    }
    pub fn FinishBuffer(__builder: *Builder, root: u32) !void {
        return __builder.Finish(root);
    }

    pub fn FinishSizePrefixedBuffer(__builder: *Builder, root: u32) !void {
        return __builder.FinishSizePrefixed(root);
    }
};