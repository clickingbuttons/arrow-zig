// Dict means store indices in values to child array's values.
// I've decided that children should handle null entries instead of dicts.
const std = @import("std");
const array = @import("./array.zig");
const tags = @import("../tags.zig");
const AnyBuilder = @import("./lib.zig").Builder;

const Array = array.Array;
const log = std.log.scoped(.arrow);

pub const BuilderError = error{
    InvalidIndexType,
};

pub const DictOptions = struct {
    nullable: bool,
    index: tags.DictOptions.Index = .i32,
    max_load_percentage: u64 = std.hash_map.default_max_load_percentage,
};

// Context and max_load_percentage match std.HashMap.
pub fn BuilderAdvanced(
    comptime ChildBuilder: type,
    comptime Context: type,
    comptime opts: DictOptions,
) type {
    const IndexType = opts.index.Type();
    const IndexList = std.ArrayListAligned(IndexType, Array.buffer_alignment);

    const NullCount = if (opts.nullable) usize else void;
    const ValidityList = if (opts.nullable) std.bit_set.DynamicBitSet else void;

    const AppendType = ChildBuilder.Type();
    const HashMap = std.HashMap(AppendType, IndexType, Context, opts.max_load_percentage);

    return struct {
        const Self = @This();

        null_count: NullCount,
        validity: ValidityList,
        indices: IndexList,
        hashmap: HashMap,
        child: ChildBuilder,

        pub fn Type() type {
            return AppendType;
        }

        pub fn init(allocator: std.mem.Allocator) !Self {
            return .{
                .null_count = if (NullCount != void) 0 else {},
                .validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
                .indices = IndexList.init(allocator),
                .hashmap = HashMap.init(allocator),
                .child = try ChildBuilder.init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            if (ValidityList != void) self.validity.deinit();
            self.indices.deinit();
            self.hashmap.deinit();
            self.child.deinit();
        }

        // Null means insert a null item into the child array.
        pub fn append(self: *Self, value: AppendType) std.mem.Allocator.Error!void {
            if (ValidityList != void) try self.validity.resize(self.validity.capacity() + 1, true);
            const count: IndexType = @intCast(self.hashmap.count());
            const get_res = try self.hashmap.getOrPut(value);
            const index = if (get_res.found_existing) get_res.value_ptr.* else count;
            if (!get_res.found_existing) {
                get_res.value_ptr.* = count;
                try self.child.append(value);
            }
            try self.indices.append(index);
        }

        // Insert a null item into the dictionary's validity.
        pub fn appendNull(self: *Self) std.mem.Allocator.Error!void {
            if (ValidityList != void) {
                try self.validity.resize(self.validity.capacity() + 1, false);
                try self.indices.append(0);
                self.null_count += 1;
            } else {
                @compileError("cannot appendNull to non-nullable dictionary. try append(null) instead.");
            }
        }

        fn shrunkIndexType(self: *Self) !tags.DictOptions.Index {
            if (self.hashmap.count() < std.math.maxInt(i8)) {
                return .i8;
            } else if (self.hashmap.count() < std.math.maxInt(i16)) {
                return .i16;
            } else if (self.hashmap.count() < std.math.maxInt(i32)) {
                return .i32;
            }
            log.err("expected {d} or fewer hashmap values, got {d}", .{ std.math.maxInt(i32), self.hashmap.count() });
            return BuilderError.InvalidIndexType;
        }

        fn shrinkIndexTo(self: *Self, comptime T: type) !Array.Buffer {
            const indices = if (T == opts.index.Type())
                try self.indices.toOwnedSlice()
            else brk: {
                const allocator = self.indices.allocator;
                var res = try std.ArrayListAligned(T, Array.buffer_alignment).initCapacity(allocator, self.indices.items.len);
                defer res.deinit();
                for (self.indices.items) |index| res.appendAssumeCapacity(@as(T, @intCast(index)));
                self.indices.deinit();
                break :brk try res.toOwnedSlice();
            };

            return std.mem.sliceAsBytes(indices);
        }

        fn shrinkIndex(self: *Self, index: tags.DictOptions.Index) !Array.Buffer {
            return switch (index) {
                .i8 => try self.shrinkIndexTo(i8),
                .i16 => try self.shrinkIndexTo(i16),
                .i32 => try self.shrinkIndexTo(i32),
            };
        }

        pub fn finish(self: *Self) !*Array {
            const allocator = self.hashmap.allocator;
            const children = try allocator.alloc(*Array, 1);
            const length = self.indices.items.len;
            const shrunk_index = try self.shrunkIndexType();
            children[0] = try self.child.finish();
            children[0].name = "dict values";
            self.hashmap.deinit();
            var res = try Array.init(allocator);
            res.* = .{
                .tag = tags.Tag{ .Dictionary = .{ .index = shrunk_index } },
                .name = @typeName(AppendType) ++ " builder",
                .allocator = allocator,
                .length = length,
                .null_count = if (NullCount != void) self.null_count else 0,
                .buffers = .{
                    if (ValidityList != void)
                        try array.validity(allocator, &self.validity, self.null_count)
                    else
                        &.{},
                    try self.shrinkIndex(shrunk_index),
                    &.{},
                },
                .children = children,
            };
            return res;
        }
    };
}

pub fn getAutoHashFn(comptime K: type, comptime Context: type) (fn (Context, K) u64) {
    return struct {
        fn hash(ctx: Context, key: K) u64 {
            _ = ctx;
            var hasher = std.hash.Wyhash.init(0);
            std.hash.autoHashStrat(&hasher, key, .Deep); // Look at slice contents.
            return hasher.final();
        }
    }.hash;
}

pub fn getAutoEqlFn(comptime K: type, comptime Context: type) (fn (Context, K, K) bool) {
    return struct {
        fn eql(ctx: Context, a: K, b: K) bool {
            _ = ctx;
            return switch (@typeInfo(K)) {
                .Pointer => |info| switch (info.size) {
                    .One, .Many, .C => a == b,
                    .Slice => std.mem.eql(u8, std.mem.sliceAsBytes(a), std.mem.sliceAsBytes(b)),
                },
                else => std.meta.eql(a, b),
            };
        }
    }.eql;
}

pub fn AutoContext(comptime K: type) type {
    return struct {
        pub const hash = getAutoHashFn(K, @This());
        pub const eql = getAutoEqlFn(K, @This());
    };
}

const flat = @import("./flat.zig");
test "init + deinit string" {
    const T = []const u8;
    var b = try BuilderAdvanced(
        flat.Builder(T),
        AutoContext(T),
        .{ .index = .i8, .nullable = false },
    ).init(std.testing.allocator);
    defer b.deinit();

    try b.append("asdf");
    try b.append("ff");
    try b.append("asdf");
    try b.append("ff");
    try b.append("gg");

    try std.testing.expectEqual(@as(usize, 3), b.hashmap.count());
}

const struct_ = @import("./struct.zig");
test "init + deinit struct" {
    const T = struct {
        a: ?i16,
        b: ?i32,
    };
    var b = try BuilderAdvanced(
        struct_.Builder(T),
        AutoContext(T),
        .{ .index = .i8, .nullable = false },
    ).init(std.testing.allocator);
    defer b.deinit();

    try b.append(.{ .a = 4, .b = 1 });
    try b.append(T{ .a = 4, .b = 1 });
    try b.append(.{ .a = 4, .b = 2 });
    try b.append(T{ .a = 4, .b = 2 });

    try std.testing.expectEqual(@as(usize, 2), b.hashmap.count());
}

test "finish" {
    const T = ?i8;
    const child_tag = tags.Tag{ .Int = tags.IntOptions{ .nullable = true, .signed = true, .bit_width = ._8 } };
    var b = try BuilderAdvanced(
        flat.Builder(T),
        AutoContext(T),
        .{ .index = .i8, .nullable = false },
    ).init(std.testing.allocator);
    try b.append(null);
    try b.append(1);

    const a = try b.finish();
    defer a.deinit();

    const offsets = std.mem.bytesAsSlice(i8, a.buffers[1]);
    try std.testing.expectEqualSlices(i8, &[_]i8{ 0, 1 }, offsets);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 1 }, a.children[0].buffers[1][0..2]);
    try std.testing.expectEqual(child_tag, a.children[0].tag);
}

pub fn Builder(comptime T: type) type {
    return BuilderAdvanced(
        AnyBuilder(T),
        AutoContext(T),
        .{ .index = .i32, .nullable = @typeInfo(T) == .Optional },
    );
}

test "convienence finish" {
    const T = ?u8;
    var b = try Builder(T).init(std.testing.allocator);
    try b.appendNull();
    try b.append(1);
    try b.append(1);

    const a = try b.finish();
    defer a.deinit();
}

test "convienence string" {
    var b = try Builder(?[]const u8).init(std.testing.allocator);
    try b.append("asdf");
    try b.append("hello");
    try b.append(null);

    try std.testing.expectEqual(@as(usize, 3), b.indices.items.len);

    const a = try b.finish();
    defer a.deinit();
}
