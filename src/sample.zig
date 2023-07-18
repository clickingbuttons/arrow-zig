// A sample array with every data type.
const std = @import("std");
const tags = @import("./tags.zig");
const Array = @import("./array/array.zig").Array;
const Builder = @import("./array/lib.zig").Builder;
const FlatBuilder = @import("./array/flat.zig").BuilderAdvanced;
const StructBuilder = @import("./array/struct.zig").BuilderAdvanced;
const UnionBuilder = @import("./array/union.zig").BuilderAdvanced;
const DictBuilder = @import("./array/dict.zig").Builder;

const Allocator = std.mem.Allocator;

// A bunch of arrays that have length 4.
pub fn flat(allocator: Allocator) !*Array {
    var b = try Builder(?i16).init(allocator);
    try b.append(null);
    try b.append(32);
    try b.append(33);
    try b.append(34);
    return try b.finish();
}

pub fn fixedFlat(allocator: Allocator) !*Array {
    const T = [3]u8;
    var b = try Builder(?T).init(allocator);
    try b.append(null);
    try b.append("hey".*);
    try b.append("guy".*);
    try b.append("bye".*);
    return try b.finish();
}

pub fn variableFlat(allocator: Allocator) !*Array {
    var b = try Builder(?[]const u8).init(allocator);
    try b.append(null);
    try b.append("hello");
    try b.append("friend");
    try b.append("goodbye");
    return try b.finish();
}

pub fn struct_(allocator: Allocator) !*Array {
    const T = struct {
        a: ?i32,
        b: ?u64,
    };
    var b = try Builder(?T).init(allocator);
    try b.append(null);
    try b.append(.{ .a = 1, .b = 1 });
    try b.append(.{ .a = 2, .b = 4 });
    try b.append(T{ .a = null, .b = 9 });
    return try b.finish();
}

pub fn denseUnion(allocator: Allocator) !*Array {
    const T = union(enum) {
        f: ?f32,
        i: ?i32,
    };
    var b = try Builder(?T).init(allocator);
    try b.append(null);
    try b.append(.{ .f = 1 });
    try b.append(.{ .f = 3 });
    try b.append(.{ .i = 5 });
    return try b.finish();
}

pub fn sparseUnion(allocator: Allocator) !*Array {
    var b = try UnionBuilder(
        struct {
            f: Builder(?f32),
            i: Builder(?i32),
        },
        .{ .nullable = true, .dense = false },
        void,
    ).init(allocator);
    try b.append(null);
    try b.append(.{ .f = 1 });
    try b.append(.{ .f = 3 });
    try b.append(.{ .i = 5 });
    return try b.finish();
}

pub fn dict(allocator: Allocator) !*Array {
    var b = try DictBuilder(?[]const u8).init(allocator);
    try b.appendNull();
    try b.append("hello");
    try b.append("there");
    try b.append("friend");
    return try b.finish();
}

pub fn map(allocator: Allocator) !*Array {
    const V = i32;
    const T = struct { []const u8, ?V };
    var b = try Builder(?T).init(allocator);
    try b.append(null);
    try b.append(.{ "hello", 1 });
    try b.appendSlice(&[_]T{ .{ "arrow", 2 }, .{ "map", null } });
    try b.append(T{ "goodbye", 3 });
    return try b.finish();
}

pub fn all(allocator: Allocator) !*Array {
    const arr_children = [_]*Array{
        try flat(allocator),
        try variableFlat(allocator),
        try struct_(allocator),
        try denseUnion(allocator),
        try sparseUnion(allocator),
        try dict(allocator),
        try map(allocator),
    };
    const length = arr_children[0].length;
    inline for (0..arr_children.len) |i| {
        arr_children[i].name = std.fmt.comptimePrint("{c}", .{'a' + @as(u8, @intCast(i))});
    }

    var children = try allocator.alloc(*Array, arr_children.len);
    for (0..arr_children.len) |i| children[i] = arr_children[i];

    var validity = try allocator.alignedAlloc(u8, Array.buffer_alignment, 1);
    validity[0] = 0b1110;

    var res = try Array.init(allocator);
    res.* = .{
        .tag = tags.Tag{ .Struct = .{ .nullable = true } },
        .name = "sample dataframe",
        .allocator = allocator,
        .length = length,
        .null_count = 1,
        .buffers = .{
            validity,
            &.{},
            &.{},
        },
        .children = children,
    };
    return res;
}

test "all struct members same length" {
    const arr = try all(std.testing.allocator);
    defer arr.deinit();

    const length = arr.children[0].length;
    for (arr.children) |child| try std.testing.expectEqual(length, child.length);
}

fn nBuffers(arr: *Array) usize {
    var res: usize = 0;
    // std.debug.print("tag {any} bufs {d}\n", .{ arr.tag, arr.tag.abiLayout().nBuffers() });
    res += arr.tag.abiLayout().nBuffers();
    if (arr.tag.abiLayout() != .Dictionary) {
        for (arr.children) |c| {
            res += nBuffers(c);
        }
    }
    return res;
}

fn nNodes(arr: *Array) usize {
    var res: usize = 0;
    res += 1;
    for (arr.children) |c| {
        res += nNodes(c);
    }
    if (arr.tag.abiLayout() == .Dictionary) {
        res -= 1; // We store in children, spec uses separate field
    }
    return res;
}

fn testArray(arr: *Array, expected_n_nodes: usize, expected_n_bufs: usize) !void {
    try std.testing.expectEqual(expected_n_nodes, nNodes(arr));
    try std.testing.expectEqual(expected_n_bufs, nBuffers(arr));
}

test "array abi layout" {
    const arr = try all(std.testing.allocator);
    defer arr.deinit();
    try std.testing.expectEqual(@as(usize, 10), arr.children.len);

    // Tested against pyarrow
    try testArray(arr.children[0], 1, 2);
    try testArray(arr.children[1], 2, 3);
    try testArray(arr.children[2], 1, 3);
    try testArray(arr.children[3], 2, 4);
    try testArray(arr.children[4], 2, 3);
    try testArray(arr.children[5], 3, 5);
    try testArray(arr.children[6], 3, 6);
    try testArray(arr.children[7], 3, 5);
    try testArray(arr.children[8], 1, 2);
    try testArray(arr.children[9], 4, 8);
}
