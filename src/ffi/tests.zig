const std = @import("std");
const abi = @import("abi.zig");
const null_array = @import("../array/array.zig").null_array;
const Builder = @import("../array/builder.zig").Builder;
const BuilderAdvanced = @import("../array/union.zig").BuilderAdvanced;
const ImportedArray = @import("./import.zig").ImportedArray;

test "flat export" {
	var b = try Builder(?[]const u8).init(std.testing.allocator);
	try b.append(null);
	try b.append("hello");

	var a = try b.finish();
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
}

test "struct export" {
	const T = struct {
		val: ?i32,
	};
	var b = try Builder(?T).init(std.testing.allocator);

	try b.append(null);
	try b.append(.{ .val = 1 });
	try b.append(T{ .val = 2 });
	try b.append(T{ .val = null });

	var a = try b.finish();
	var c = try abi.Array.init(a);
	defer c.release.?(&c);

	var cname = "table 1";
	a.name = cname;
	var s = try abi.Schema.init(a);
	defer s.release.?(&s);
	try std.testing.expectEqualStrings(cname, s.name.?[0..cname.len]);
}

test "list export" {
	var b = try Builder(?[]const i8).init(std.testing.allocator);
	try b.append(null);
	try b.append(&[_]i8{1,2,3});

	const a = try b.finish();

	var c = try abi.Array.init(a);
	defer c.release.?(&c);
	var s = try abi.Schema.init(a);
	defer s.release.?(&s);
}

test "nullable dense union export" {
	// Straight from example
	const ChildrenBuilders = struct {
		f: Builder(?f32),
		i: Builder(?i32),
	};
	var b = try BuilderAdvanced(ChildrenBuilders, .{ .nullable = true, .dense = true }, void).init(std.testing.allocator);

	try b.append(.{ .f = 1.2 });
	try b.append(null);
	try b.append(.{ .f = 3.4 });
	try b.append(.{ .i = 5 });

	var a = try b.finish();
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

test "null array export" {
	var n = null_array;
	var c = try abi.Array.init(&n);
	defer c.release.?(&c);
	try std.testing.expectEqual(@as(i64, 0), c.null_count);

	var s = try abi.Schema.init(&n);
	defer s.release.?(&s);
	try std.testing.expectEqualStrings("n\x00", s.format[0..2]);
	try std.testing.expectEqual(@as(i64, 0), s.n_children);
}

test "flat import" {
	std.testing.log_level = .debug;
	const allocator = std.testing.allocator;
	var b = try Builder(?[3]u8).init(allocator);
	try b.append(null);
	const s = "hey";
	try b.append(std.mem.sliceAsBytes(s)[0..s.len].*);

	var a = try b.finish();
	var abi_schema = try abi.Schema.init(a);
	var abi_arr = try abi.Array.init(a);

	var imported = try ImportedArray.init(allocator, abi_arr, abi_schema);
	defer imported.deinit();

	const a2 = imported.array;
	try std.testing.expectEqual(@as(u8, 0b10), a2.buffers[0][0]);
	try std.testing.expectEqualStrings("\x00" ** s.len ++ s, a2.buffers[1]);
	try std.testing.expectEqual(@as(usize, 0), a2.buffers[2].len);
}
