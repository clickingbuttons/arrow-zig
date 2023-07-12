// List means single child. Includes List and Fixed-Size List layouts.
const std = @import("std");
const array = @import("./array.zig");
const tags = @import("../tags.zig");
const builder = @import("./builder.zig");

const Array = array.Array;

pub fn BuilderAdvanced(
    comptime ChildBuilder: type,
    comptime opts: tags.ListOptions,
    comptime fixed_len: i32,
) type {
    const tag: tags.Tag = if (fixed_len == 0)
        .{ .List = opts }
    else
        .{ .FixedList = .{ .nullable = opts.nullable, .fixed_len = fixed_len } };
    const is_fixed = tag == .FixedList;

    const NullCount = if (opts.nullable) usize else void;
    const ValidityList = if (opts.nullable) std.bit_set.DynamicBitSet else void;

    const OffsetType = if (opts.large and tag != .FixedList) i64 else i32;
    const OffsetList = if (!is_fixed) std.ArrayListAligned(OffsetType, Array.buffer_alignment) else void;

    const ChildAppendType = ChildBuilder.Type();
    const AppendType = switch (opts.nullable) {
        true => if (is_fixed) ?[fixed_len]ChildAppendType else ?[]const ChildAppendType,
        false => if (is_fixed) [fixed_len]ChildAppendType else []const ChildAppendType,
    };

    return struct {
        const Self = @This();

        null_count: NullCount,
        validity: ValidityList,
        offsets: OffsetList,
        child: ChildBuilder,

        pub fn Type() type {
            return AppendType;
        }

        pub fn init(allocator: std.mem.Allocator) !Self {
            var res = Self{
                .null_count = if (NullCount != void) 0 else {},
                .validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
                .offsets = if (OffsetList != void) OffsetList.init(allocator) else {},
                .child = try ChildBuilder.init(allocator),
            };
            if (OffsetList != void) {
                try res.offsets.append(0);
            }

            return res;
        }

        pub fn deinit(self: *Self) void {
            if (ValidityList != void) self.validity.deinit();
            if (OffsetList != void) self.offsets.deinit();
            self.child.deinit();
        }

        fn appendAny(self: *Self, value: anytype) std.mem.Allocator.Error!void {
            return switch (@typeInfo(@TypeOf(value))) {
                .Null => {
                    if (OffsetList != void) {
                        try self.offsets.append(self.offsets.getLast());
                    } else {
                        const to_append = if (comptime @typeInfo(ChildAppendType) == .Optional) null else 0;
                        if (comptime is_fixed) {
                            for (0..fixed_len) |_| try self.child.append(to_append);
                        } else {
                            try self.child.append(to_append);
                        }
                    }
                },
                .Optional => {
                    try self.validity.resize(self.validity.capacity() + 1, value != null);
                    if (value) |v| {
                        try self.appendAny(v);
                    } else {
                        self.null_count += 1;
                        try self.appendAny(null);
                    }
                },
                .Pointer => |p| switch (p.size) {
                    .Slice => {
                        if (tag != .List) @compileError("cannot append slice to non-list type");
                        for (value) |v| try self.child.append(v);
                        try self.offsets.append(@intCast(self.child.values.items.len));
                    },
                    else => |t| @compileError("unsupported pointer type " ++ @tagName(t)),
                },
                .Array => |a| {
                    std.debug.assert(a.len == fixed_len);
                    for (value) |v| try self.child.append(v);
                },
                else => |t| @compileError("unsupported append type " ++ @tagName(t)),
            };
        }

        pub fn append(self: *Self, value: AppendType) std.mem.Allocator.Error!void {
            return self.appendAny(value);
        }

        fn len(self: *Self) usize {
            if (OffsetList != void) return self.offsets.items.len - 1;
            var res = self.child.values.items.len;
            if (comptime is_fixed) res /= @as(usize, @intCast(fixed_len));
            return res;
        }

        pub fn finish(self: *Self) !*Array {
            const length = self.len();
            const allocator = self.child.values.allocator;
            const children = try allocator.alloc(*Array, 1);
            children[0] = try self.child.finish();

            var res = try Array.init(allocator);
            res.* = .{
                .tag = tag,
                .name = @typeName(AppendType) ++ " builder",
                .allocator = allocator,
                .length = length,
                .null_count = if (NullCount != void) self.null_count else 0,
                .buffers = .{
                    if (ValidityList != void)
                        try array.validity(allocator, &self.validity, self.null_count)
                    else
                        &.{},
                    if (OffsetList != void)
                        std.mem.sliceAsBytes(try self.offsets.toOwnedSlice())
                    else
                        &.{},
                    &.{},
                },
                .children = children,
            };
            return res;
        }
    };
}

pub fn Builder(comptime Slice: type) type {
    const nullable = @typeInfo(Slice) == .Optional;
    const Child = if (nullable) @typeInfo(Slice).Optional.child else Slice;
    const t = @typeInfo(Child);
    if (!(t == .Pointer and t.Pointer.size == .Slice) and t != .Array) {
        @compileError(@typeName(Slice) ++ " is not a slice or array type");
    }
    const arr_len = if (t == .Array) t.Array.len else 0;
    const ChildBuilder = builder.Builder(if (t == .Pointer) t.Pointer.child else t.Array.child);
    return BuilderAdvanced(ChildBuilder, .{ .nullable = nullable, .large = false }, arr_len);
}

test "init + deinit optional child and parent" {
    var b = try Builder([]const ?i8).init(std.testing.allocator);
    defer b.deinit();

    try b.append(&[_]?i8{ 1, null, 3 });
}

test "init + deinit varbinary" {
    var b = try Builder(?[][]const u8).init(std.testing.allocator);
    defer b.deinit();

    try b.append(null);
    try b.append(&[_][]const u8{ "hello", "goodbye" });
}

test "finish" {
    var b = try Builder(?[]const i8).init(std.testing.allocator);
    try b.append(null);
    try b.append(&[_]i8{ 1, 2, 3 });

    const a = try b.finish();
    defer a.deinit();

    try std.testing.expectEqual(@as(u8, 0b10), a.buffers[0][0]);
    const offsets = std.mem.bytesAsSlice(i32, a.buffers[1]);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 0, 0, 3 }, offsets);
}

test "fixed" {
    var b = try Builder([3]?i8).init(std.testing.allocator);
    defer b.deinit();

    try b.append([_]?i8{ 1, null, 3 });
}

test "fixed finish" {
    var b = try Builder(?[3]?i8).init(std.testing.allocator);
    try b.append([_]?i8{ 1, 2, 3 });
    try b.append(null);
    try b.append([_]?i8{ 4, null, 6 });

    const a = try b.finish();
    defer a.deinit();

    try std.testing.expectEqual(@as(usize, 3), a.length);
    try std.testing.expectEqual(@as(usize, 1), a.null_count);
    try std.testing.expectEqual(@as(u8, 0b101), a.buffers[0][0]);
    try std.testing.expectEqual(@as(usize, 0), a.buffers[1].len); // No offsets
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3, 0, 0, 0 }, a.children[0].buffers[1][0..6]);
}
