const std = @import("std");
const abi = @import("abi.zig");
const ImportedArray = @import("./import.zig").ImportedArray;
const array_mod = @import("../array/array.zig");
const Builder = @import("../array/builder.zig").Builder;
const union_ = @import("../array/union.zig");
const dict = @import("../array/dict.zig");
const map = @import("../array/map.zig");

const Array = array_mod.Array;
const allocator = std.testing.allocator;

fn testExport(array: *Array, comptime format_string: []const u8) !void {
	var abi_arr = try abi.Array.init(array);
	defer abi_arr.release.?(&abi_arr);

	var abi_schema = try abi.Schema.init(array);
	defer abi_schema.release.?(&abi_schema);
	try std.testing.expectEqualStrings(format_string ++ "\x00", abi_schema.format[0..format_string.len + 1]);
}

fn testImport(array: *Array) !void {
	var abi_schema = try abi.Schema.init(array);
	var abi_arr = try abi.Array.init(array);
	var imported = try ImportedArray.init(allocator, abi_arr, abi_schema);
	defer imported.deinit();
}

fn sampleFlat() !*Array {
	var b = try Builder(?i16).init(allocator);
	try b.append(null);
	try b.append(32);
	return try b.finish();
}

fn sampleFixedFlat() !*Array {
	var b = try Builder(?[3]u8).init(allocator);
	try b.append(null);
	const s = "hey";
	try b.append(std.mem.sliceAsBytes(s)[0..s.len].*);
	return try b.finish();
}

fn sampleVariableFlat() !*Array {
	var b = try Builder(?[]const u8).init(allocator);
	try b.append(null);
	try b.append("hello");
	return try b.finish();
}

fn sampleList() !*Array {
	const T = i16;
	var b = try Builder(?[]const T).init(allocator);
	try b.append(null);
	try b.append(&[_]T{1,2,3});
	return try b.finish();
}

fn sampleStruct() !*Array {
	const T = struct {
		a: ?i32,
		b: ?u64,
	};
	var b = try Builder(?T).init(allocator);
	try b.append(null);
	try b.append(.{ .a = 1, .b = 1 });
	try b.append(T{ .a = 2, .b = 4 });
	try b.append(T{ .a = null, .b = 9 });
	return try b.finish();
}

fn sampleDenseUnion() !*Array {
	// Straight from example
	const T = union(enum) {
		f: ?f32,
		i: ?i32,
	};
	var b = try Builder(?T).init(allocator);
	try b.append(.{ .f = 1.2 });
	try b.append(null);
	try b.append(.{ .f = 3.4 });
	try b.append(.{ .i = 5 });
	return try b.finish();
}

fn sampleSparseUnion() !*Array {
	// Straight from example
	const ChildrenBuilders = struct {
		f: Builder(?f32),
		i: Builder(?i32),
	};
	var b = try union_.BuilderAdvanced(ChildrenBuilders, .{ .nullable = true, .dense = false }, void).init(allocator);
	try b.append(.{ .f = 1.2 });
	try b.append(null);
	try b.append(.{ .f = 3.4 });
	try b.append(.{ .i = 5 });
	return try b.finish();
}

fn sampleDict() !*Array {
	var b = try dict.Builder(?[]const u8).init(std.testing.allocator);
	try b.append("asdf");
	try b.append("hello");
	try b.append(null);
	return try b.finish();
}

fn sampleMap() !*Array {
	const V = i32;
	const T = struct { []const u8, ?V };
	var b = try map.Builder(?T).init(std.testing.allocator);
	try b.append(null);
	try b.append(.{ "hello", 1 });
	try b.append(T{ "goodbye", 2 });
	try b.appendSlice(&[_]T{ .{ "arrow", 2 }, .{ "map", null } });
	return try b.finish();
}

test "null export" {
	var n = array_mod.null_array;
	try testExport(&n, "n");
}

test "flat export" {
	var a = try sampleFlat();
	try testExport(a, "s");
}

test "fixed flat export" {
	var a = try sampleFixedFlat();
	try testExport(a, "w:3");
}

test "flat variable export" {
	var a = try sampleVariableFlat();

	var c = try abi.Array.init(a);
	defer c.release.?(@constCast(&c));

	const buf0 = @constCast(c.buffers.?[0].?);
	try std.testing.expectEqual(@as(u8, 0b10), @ptrCast([*]u8, buf0)[0]);
	try std.testing.expectEqual(@as(i64, 1), c.null_count);

	const buf1 = @constCast(c.buffers.?[1].?);
	const offsets = @ptrCast([*]i32, @alignCast(@alignOf(i32), buf1));
	try std.testing.expectEqual(@as(i32, 0), offsets[0]);
	try std.testing.expectEqual(@as(i32, 0), offsets[1]);
	try std.testing.expectEqual(@as(i32, 5), offsets[2]);

	const buf2 = @constCast(c.buffers.?[2].?);
	const values = @ptrCast([*]u8, buf2);
	try std.testing.expectEqualStrings("hello", values[0..5]);

	var cname = "c1";
	a.name = cname;
	var s = try abi.Schema.init(a);
	defer s.release.?(@constCast(&s));
	try std.testing.expectEqualStrings(cname, s.name.?[0..cname.len]);
	try std.testing.expectEqualStrings("z\x00", s.format[0..2]);
}

test "list export" {
	var a = try sampleList();
	try testExport(a, "+l");
}

test "struct export" {
	var a = try sampleStruct();
	try testExport(a, "+s");
}

test "dense union export" {
	var a = try sampleDenseUnion();

	var arr = try abi.Array.init(a);
	defer arr.release.?(@constCast(&arr));

	try std.testing.expectEqual(@as(i64, 2), arr.n_buffers);
	try std.testing.expectEqual(@as(i64, 2), arr.n_children);
	const values = @ptrCast([*]const u8, arr.buffers.?[0].?);
	const validity = @ptrCast([*]const u8, arr.children.?[0].buffers.?[0].?);
	try std.testing.expectEqual(@as(u8, 0), values[0]);
	try std.testing.expectEqual(@as(u8, 0), values[1]);
	try std.testing.expectEqual(@as(u8, 0), values[2]);
	try std.testing.expectEqual(@as(u8, 1), values[3]);
	try std.testing.expectEqual(@as(u8, 0b0101), validity[0]);

	const s = try abi.Schema.init(a);
	defer s.release.?(@constCast(&s));

	try std.testing.expectEqualStrings("+ud:0,1\x00", s.format[0..8]);
}

test "sparse union export" {
	var a = try sampleSparseUnion();
	try testExport(a, "+us:0,1");
}

test "dict export" {
	var a = try sampleDict();
	try testExport(a, "c");
}

test "map export" {
	var a = try sampleMap();
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
	const s = "hey";
	var a = try sampleFixedFlat();
	var abi_schema = try abi.Schema.init(a);
	var abi_arr = try abi.Array.init(a);

	var imported = try ImportedArray.init(allocator, abi_arr, abi_schema);
	defer imported.deinit();

	const a2 = imported.array;
	try std.testing.expectEqual(@as(u8, 0b10), a2.buffers[0][0]);
	try std.testing.expectEqualStrings("\x00" ** s.len ++ s, a2.buffers[1]);
	try std.testing.expectEqual(@as(usize, 0), a2.buffers[2].len);
}

test "fixed flat import" {
	var a = try sampleFixedFlat();
	try testImport(a);
}

test "flat variable import" {
	var a = try sampleVariableFlat();
	try testImport(a);
}

test "list import" {
	var a = try sampleList();
	try testImport(a);
}

test "struct import" {
	var a = try sampleStruct();
	try testImport(a);
}

test "dense union import" {
	var a = try sampleDenseUnion();
	try testImport(a);
}

test "sparse union import" {
	var a = try sampleSparseUnion();
	try testImport(a);
}

test "dict import" {
	var a = try sampleDict();
	try testImport(a);
}

test "map import" {
	var a = try sampleMap();
	try testImport(a);
}
