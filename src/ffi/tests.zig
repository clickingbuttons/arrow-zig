const std = @import("std");
const abi = @import("abi.zig");
const ImportedArray = @import("./import.zig").ImportedArray;
const array_mod = @import("../array/array.zig");
const Builder = @import("../array/lib.zig").Builder;
const union_ = @import("../array/union.zig");
const dict = @import("../array/dict.zig");
const map = @import("../array/map.zig");
const sample = @import("../sample.zig");

const Array = array_mod.Array;
const allocator = std.testing.allocator;

fn testExport(array: *Array, comptime format_string: []const u8) !void {
    var abi_arr = try abi.Array.init(array);
    defer abi_arr.release.?(&abi_arr);

    var abi_schema = try abi.Schema.init(array);
    defer abi_schema.release.?(&abi_schema);
    try std.testing.expectEqualStrings(
        format_string ++ "\x00",
        abi_schema.format[0 .. format_string.len + 1],
    );
}

fn testImport(array: *Array) !void {
    var abi_schema = try abi.Schema.init(array);
    var abi_arr = try abi.Array.init(array);
    var imported = try ImportedArray.init(allocator, abi_arr, abi_schema);
    defer imported.deinit();
}

test "null export" {
    var n = array_mod.null_array;
    try testExport(&n, "n");
}

test "flat export" {
    var a = try sample.flat(allocator);
    try testExport(a, "s");
}

test "fixed flat export" {
    var a = try sample.fixedFlat(allocator);
    try testExport(a, "w:3");
}

test "flat variable export" {
    var a = try sample.variableFlat(allocator);

    var c = try abi.Array.init(a);
    defer c.release.?(@constCast(&c));

    const buf0: [*]u8 = @ptrCast(@constCast(c.buffers.?[0].?));
    try std.testing.expectEqual(@as(u8, 0b1110), buf0[0]);
    try std.testing.expectEqual(@as(i64, 1), c.null_count);

    const buf1 = @constCast(c.buffers.?[1].?);
    const offsets: [*]i32 = @ptrCast(@alignCast(buf1));
    try std.testing.expectEqual(@as(i32, 0), offsets[0]);
    try std.testing.expectEqual(@as(i32, 0), offsets[1]);
    const s = "hello";
    try std.testing.expectEqual(@as(i32, s.len), offsets[2]);

    const buf2 = @constCast(c.buffers.?[2].?);
    const values: [*]u8 = @ptrCast(buf2);
    try std.testing.expectEqualStrings("hello", values[0..s.len]);

    var cname = "c1";
    a.name = cname;
    var schema = try abi.Schema.init(a);
    defer schema.release.?(@constCast(&schema));
    try std.testing.expectEqualStrings(cname, schema.name.?[0..cname.len]);
    try std.testing.expectEqualStrings("z\x00", schema.format[0..2]);
}

test "list export" {
    var a = try sample.list(allocator);
    try testExport(a, "+l");
}

test "fixed list export" {
    var a = try sample.fixedList(allocator);
    try testExport(a, "+w:3");
}

test "struct export" {
    var a = try sample.struct_(allocator);
    try testExport(a, "+s");
}

test "dense union export" {
    var a = try sample.denseUnion(allocator);
    try testExport(a, "+ud:0,1");
}

test "sparse union export" {
    var a = try sample.sparseUnion(allocator);
    try testExport(a, "+us:0,1");
}

test "dict export" {
    var a = try sample.dict(allocator);
    try testExport(a, "c");
}

test "map export" {
    var a = try sample.map(allocator);
    try testExport(a, "+m");
}

test "null import" {
    var a = array_mod.null_array;
    var c = try abi.Array.init(&a);
    var s = try abi.Schema.init(&a);

    var imported = try ImportedArray.init(allocator, c, s);
    defer imported.deinit();
}

test "flat import" {
    const s1 = "hey";
    const s2 = "guy";
    const s3 = "bye";
    var a = try sample.fixedFlat(allocator);
    var abi_schema = try abi.Schema.init(a);
    var abi_arr = try abi.Array.init(a);

    var imported = try ImportedArray.init(allocator, abi_arr, abi_schema);
    defer imported.deinit();

    const a2 = imported.array;
    try std.testing.expectEqual(@as(u8, 0b1110), a2.buffers[0][0]);
    try std.testing.expectEqualStrings("\x00" ** s1.len ++ s1 ++ s2 ++ s3, a2.buffers[1]);
    try std.testing.expectEqual(@as(usize, 0), a2.buffers[2].len);
}

test "fixed flat import" {
    var a = try sample.fixedFlat(allocator);
    try testImport(a);
}

test "flat variable import" {
    var a = try sample.variableFlat(allocator);
    try testImport(a);
}

test "list import" {
    var a = try sample.list(allocator);
    try testImport(a);
}

test "fixed list import" {
    var a = try sample.fixedList(allocator);
    try testImport(a);
}

test "struct import" {
    var a = try sample.struct_(allocator);
    try testImport(a);
}

test "dense union import" {
    var a = try sample.denseUnion(allocator);
    try testImport(a);
}

test "sparse union import" {
    var a = try sample.sparseUnion(allocator);
    try testImport(a);
}

test "dict import" {
    var a = try sample.dict(allocator);
    try testImport(a);
}

test "map import" {
    var a = try sample.map(allocator);
    try testImport(a);
}

test "all import" {
    var a = try sample.all(allocator);
    try testImport(a);
}
