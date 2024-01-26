// Flat means no children. Includes Primitive, VariableBinary, and FixedBinary layouts.
const std = @import("std");
const array = @import("./array.zig");
const Array = array.Array;
const Tag = Array.Tag;

pub fn BuilderAdvanced(comptime T: type, comptime opts: Tag.BinaryOptions) type {
    const tag = Tag.fromPrimitive(T, opts);
    const layout = tag.abiLayout();
    if (layout != .Primitive and layout != .VariableBinary) {
        @compileError("unsupported flat type " ++ @typeName(T));
    }
    const is_fixed = tag == .FixedBinary;
    const fixed_len = if (is_fixed) tag.FixedBinary.fixed_len else 0;
    if (is_fixed and fixed_len < 1) {
        @compileError(std.fmt.comptimePrint("expected fixed_len >= 1, got {d}", .{fixed_len}));
    }

    const NullCount = if (@typeInfo(T) == .Optional) usize else void;
    const ValidityList = if (@typeInfo(T) == .Optional) std.bit_set.DynamicBitSet else void;
    const ValueType = tag.Primitive();

    const OffsetType = if (opts.large) i64 else i32;
    const OffsetList = if (layout.hasOffsets()) std.ArrayListAligned(OffsetType, Array.buffer_alignment) else void;
    const ValueList = std.ArrayListAligned(ValueType, Array.buffer_alignment);

    return struct {
        const Self = @This();

        null_count: NullCount,
        validity: ValidityList,
        offsets: OffsetList,
        values: ValueList,

        pub fn Type() type {
            return T;
        }

        pub fn init(allocator: std.mem.Allocator) !Self {
            var res = Self{
                .null_count = if (NullCount != void) 0 else {},
                .validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
                .offsets = if (OffsetList != void) OffsetList.init(allocator) else {},
                .values = ValueList.init(allocator),
            };
            // dunno why this is in the spec:
            // > the offsets buffer contains length + 1 signed integers (either 32-bit or 64-bit,
            // > depending on the logical type), which encode the start position of each slot in the data
            // > buffer.
            if (OffsetList != void) try res.offsets.append(0);

            return res;
        }

        pub fn deinit(self: *Self) void {
            if (ValidityList != void) self.validity.deinit();
            if (OffsetList != void) self.offsets.deinit();
            self.values.deinit();
        }

        fn appendAny(self: *Self, value: anytype) std.mem.Allocator.Error!void {
            switch (@typeInfo(@TypeOf(value))) {
                .Bool, .Int, .Float, .ComptimeInt, .ComptimeFloat => try self.values.append(value),
                .Pointer => |p| switch (p.size) {
                    .Slice => {
                        try self.values.appendSlice(value);
                        try self.offsets.append(@intCast(self.values.items.len));
                    },
                    else => |t| @compileError("unsupported pointer type " ++ @tagName(t)),
                },
                .Array => |a| {
                    if (a.len != fixed_len)
                        @compileError(
                            std.fmt.comptimePrint(
                                "expected array of len {d} but got array of len {d}",
                                .{ fixed_len, a.len },
                            ),
                        );
                    try self.values.appendSlice(&value);
                },
                .Null => {
                    if (OffsetList != void) {
                        try self.offsets.append(self.offsets.getLast());
                    } else {
                        // > Array slots which are null are not required to have a particular value; any
                        // > "masked" memory can have any value and need not be zeroed, though implementations
                        // > frequently choose to zero memory for null items.
                        // PLEASE, for the sake of SIMD, 0 this
                        if (comptime is_fixed) {
                            for (0..fixed_len) |_| try self.appendAny(0);
                        } else {
                            try self.appendAny(0);
                        }
                    }
                },
                .Optional => {
                    const is_null = value == null;
                    try self.validity.resize(self.validity.capacity() + 1, !is_null);
                    if (is_null) {
                        self.null_count += 1;
                        try self.appendAny(null);
                    } else {
                        try self.appendAny(value.?);
                    }
                },
                else => |t| @compileError("unsupported append type " ++ @tagName(t)),
            }
        }

        pub fn append(self: *Self, value: T) std.mem.Allocator.Error!void {
            return self.appendAny(value);
        }

        fn makeBufs(self: *Self) !Array.Buffers {
            const allocator = self.values.allocator;
            return switch (comptime layout) {
                .Primitive => .{
                    if (ValidityList != void)
                        try array.validity(allocator, &self.validity, self.null_count)
                    else
                        &.{},
                    std.mem.sliceAsBytes(try self.values.toOwnedSlice()),
                    &.{},
                },
                .VariableBinary => .{
                    if (ValidityList != void)
                        try array.validity(allocator, &self.validity, self.null_count)
                    else
                        &.{},
                    if (OffsetList != void)
                        std.mem.sliceAsBytes(try self.offsets.toOwnedSlice())
                    else
                        &.{},
                    std.mem.sliceAsBytes(try self.values.toOwnedSlice()),
                },
                else => @compileError("should have checked layout earlier"),
            };
        }

        fn len(self: *Self) usize {
            if (OffsetList != void) return self.offsets.items.len - 1;
            var res = self.values.items.len;
            if (comptime is_fixed) res /= @as(usize, @intCast(fixed_len));
            return res;
        }

        pub fn finish(self: *Self) !*Array {
            const allocator = self.values.allocator;
            var res = try Array.init(allocator);
            res.* = .{
                .tag = tag,
                .name = @typeName(T) ++ " builder",
                .allocator = allocator,
                .length = self.len(),
                .null_count = if (NullCount != void) self.null_count else 0,
                .buffers = try self.makeBufs(),
                .children = &.{},
            };
            return res;
        }
    };
}

pub fn Builder(comptime T: type) type {
    const nullable = @typeInfo(T) == .Optional;
    return BuilderAdvanced(T, .{ .large = false, .utf8 = false, .nullable = nullable });
}

test "primitive init + deinit" {
    var b = try Builder(i32).init(std.testing.allocator);
    defer b.deinit();

    try b.append(32);
}

const MaskInt = std.bit_set.DynamicBitSet.MaskInt;
test "primitive optional" {
    var b = try Builder(?i32).init(std.testing.allocator);
    defer b.deinit();
    try b.append(1);
    try b.append(null);
    try b.append(2);
    try b.append(4);

    const masks = b.validity.unmanaged.masks;
    try std.testing.expectEqual(@as(MaskInt, 0b1101), masks[0]);
}

test "primitive finish" {
    const T = i32;
    var b = try Builder(?T).init(std.testing.allocator);
    try b.append(1);
    try b.append(null);
    try b.append(2);
    try b.append(4);

    var a = try b.finish();
    defer a.deinit();

    const masks = a.buffers[0];
    try std.testing.expectEqual(@as(u8, 0b1101), masks[0]);

    const values = std.mem.bytesAsSlice(T, a.buffers[1]);
    try std.testing.expectEqualSlices(T, &[_]T{ 1, 0, 2, 4 }, values);

    const tag = Tag{ .Int = Tag.IntOptions{ .nullable = true, .signed = true, .bit_width = ._32 } };
    try std.testing.expectEqual(tag, a.tag);
}

test "varbinary init + deinit" {
    var b = try Builder([]const u8).init(std.testing.allocator);
    defer b.deinit();

    try b.append(&[_]u8{ 1, 2, 3 });
}

test "varbinary utf8" {
    var b = try BuilderAdvanced([]const u8, .{ .large = true, .utf8 = true, .nullable = false }).init(std.testing.allocator);
    defer b.deinit();

    try b.append(&[_]u8{ 1, 2, 3 });
}

test "varbinary optional" {
    var b = try Builder(?[]const u8).init(std.testing.allocator);
    defer b.deinit();
    try b.append(null);
    try b.append(&[_]u8{ 1, 2, 3 });

    const masks = b.validity.unmanaged.masks;
    try std.testing.expectEqual(@as(MaskInt, 0b10), masks[0]);
}

test "varbinary finish" {
    var b = try Builder(?[]const u8).init(std.testing.allocator);
    const s = "hello";
    try b.append(null);
    try b.append(s);

    var a = try b.finish();
    defer a.deinit();

    const buffers = a.buffers;
    try std.testing.expectEqual(@as(u8, 0b10), buffers[0][0]);
    const offsets = std.mem.bytesAsSlice(i32, buffers[1]);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 0, s.len }, offsets);
    try std.testing.expectEqualStrings(s, buffers[2][0..s.len]);
}

test "fixed binary finish" {
    var b = try Builder(?[3]u8).init(std.testing.allocator);
    try b.append(null);
    const s = "hey";
    try b.append(std.mem.sliceAsBytes(s)[0..s.len].*);

    var a = try b.finish();
    defer a.deinit();

    const buffers = a.buffers;
    try std.testing.expectEqual(@as(u8, 0b10), buffers[0][0]);
    try std.testing.expectEqualStrings("\x00" ** s.len ++ s, buffers[1]);
    try std.testing.expectEqual(@as(usize, 0), buffers[2].len);
}
