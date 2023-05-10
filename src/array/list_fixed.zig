const array = @import("./array.zig");
const ArrayBuilderAdvanced = @import("./list.zig").ArrayBuilderAdvanced;

pub fn ArrayBuilder(comptime Array: type) type {
	const is_nullable = @typeInfo(Array) == .Optional;
	const Child = if (is_nullable) @typeInfo(Array).Optional.child else Array;
	const t = @typeInfo(Child);
	if (t != .Array) {
		@compileError(@typeName(Array) ++ " is not an array type");
	}
	const ChildBuilder = flat.ArrayBuilder(t.Array.child);
	return ArrayBuilderAdvanced(ChildBuilder, .{ .is_nullable = is_nullable, .is_large = false }, t.Array.len);
}

const std = @import("std");
const flat = @import("./flat.zig");
const tags = @import("../tags.zig");

test "init + deinit optional child and parent" {
	var b = try ArrayBuilder([3]?i8).init(std.testing.allocator);
	defer b.deinit();

	try b.append([_]?i8{1,null,3});
}

test "finish" {
	var b = try ArrayBuilder(?[3]i8).init(std.testing.allocator);
	try b.append([_]i8{1,2,3});
	try b.append(null);

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(i64, 1), a.null_count);
	try std.testing.expectEqual(@as(u8, 1), a.children[0].values[0]);
	try std.testing.expectEqual(@as(u8, 0), a.children[0].values[3]);

	try std.testing.expectEqual(@as(array.MaskInt, 0b01), a.validity[0]);
}
