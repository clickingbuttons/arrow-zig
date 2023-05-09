const ArrayBuilderAdvanced = @import("./list.zig").ArrayBuilderAdvanced;

pub fn ArrayBuilder(comptime ChildBuilder: type, comptime len: i32) type {
	return ArrayBuilderAdvanced(ChildBuilder, .{ .is_nullable = true, .is_large = false }, len);
}

const std = @import("std");
const flat = @import("./flat.zig");

test "init + deinit optional child and parent" {
	var b = try ArrayBuilder(flat.ArrayBuilder(?i8), 3).init(std.testing.allocator);
	defer b.deinit();

	try b.append([_]?i8{1,null,3});
	try b.append(null);
}

test "finish" {
	var b = try ArrayBuilder(flat.ArrayBuilder(i8), 3).init(std.testing.allocator);
	try b.append([_]i8{1,2,3});
	try b.append(null);

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(i64, 1), a.null_count);
	try std.testing.expectEqual(@as(u8, 1), a.children[0].values[0]);
	try std.testing.expectEqual(@as(u8, 0), a.children[0].values[3]);
}
