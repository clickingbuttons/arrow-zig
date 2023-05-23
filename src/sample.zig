// A sample array with every data type.
const std = @import("std");
const array = @import("./array/array.zig");
const builder = @import("./array/builder.zig");
const flat = @import("./array/flat.zig");
const struct_ = @import("./array/struct.zig");
const dict = @import("./array/dict.zig");
const union_ = @import("./array/union.zig");

pub fn sampleArray(allocator: std.mem.Allocator) !*array.Array {
	const StructT = struct {
		a: ?i32,
		b: ?i8,
	};
	const UnionT = union(enum) {
		a: ?i32,
		b: ?i8,
	};
	const UnionChildrenBuilders = struct {
		i: flat.Builder(?i32),
		f: flat.Builder(?f32),
	};
	const T = struct {
		a: flat.BuilderAdvanced(?[]const u8, .{ .large = false, .utf8 = true }),
		b: builder.Builder(?i32),
		c: builder.Builder(?[]i32),
		d: builder.Builder(?[2]i32),
		e: builder.Builder(?StructT),
		f: builder.Builder(?UnionT),
		g: union_.BuilderAdvanced(UnionChildrenBuilders, .{ .nullable = true, .dense = false }, void),
		h: dict.Builder(?u32),
	};
	var b = try struct_.BuilderAdvanced(T, .{ .nullable = true }, void).init(allocator);
	{
		errdefer b.deinit();

		// Keep synched with integration_test.py
		try b.append(null);
		try b.append(.{
			.a = "hello",
			.b = 1,
			.c = &[_]i32{3,4},
			.d = [_]i32{3,4},
			.e = StructT{ .a = 3, .b = 6 },
			.f = UnionT{ .a = 3 },
			.g = .{ .i = 3 },
			.h = 3,
		});
		try b.append(.{
			.a = "goodbye",
			.b = 2,
			.c = &[_]i32{5,6},
			.d = [_]i32{5,6},
			.e = .{ .a = 4, .b = 7 },
			.f = .{ .b = 4 },
			.g = .{ .f = 1 },
			.h = 4,
		});
	}

	return b.finish();
}

fn nBuffers(arr: *array.Array) usize {
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

fn nNodes(arr: *array.Array) usize {
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

fn testArray(arr: *array.Array, expected_n_nodes: usize, expected_n_bufs: usize) !void {
	try std.testing.expectEqual(expected_n_nodes, nNodes(arr));
	try std.testing.expectEqual(expected_n_bufs, nBuffers(arr));
}

test "array abi layout" {
	const arr = try sampleArray(std.testing.allocator);
	defer arr.deinit();
	try std.testing.expectEqual(@as(usize, 8), arr.children.len);

	// Tested against pyarrow
	try testArray(arr.children[0], 1, 3); // 1, 3
	try testArray(arr.children[1], 1, 2); // 2, 5
	try testArray(arr.children[2], 2, 4); // 4, 9
	try testArray(arr.children[3], 2, 3); // 6, 12
	try testArray(arr.children[4], 3, 5); // 9, 17
	try testArray(arr.children[5], 3, 6); // 12, 23
	try testArray(arr.children[6], 3, 5); // 15, 28
	try testArray(arr.children[7], 1, 2); // 16, 30

	// try std.testing.expectEqual(@as(usize, 35), nArrays(arr));
	try std.testing.expectEqual(@as(usize, 31), nBuffers(arr));
}
