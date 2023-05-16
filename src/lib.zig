const std = @import("std");
const abi = @import("./abi.zig");
const array = @import("./array/array.zig");
const builder = @import("./array/builder.zig");
const flat = @import("./array/flat.zig");
const struct_ = @import("./array/struct.zig");
const dict = @import("./array/dict.zig");
const union_ = @import("./array/union.zig");

pub const Array = array.Array;
pub const Builder = builder.Builder;
pub const null_array = array.null_array;

fn sampleRecordBatch2(allocator: std.mem.Allocator, out_array: *abi.Array, out_schema: *abi.Schema) !void {
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
		a: flat.BuilderAdvanced(?[]const u8, .{ .is_large = false, .is_utf8 = true }),
		b: Builder(?i32),
		c: Builder(?[]i32),
		d: Builder(?[2]i32),
		e: Builder(?StructT),
		f: Builder(?UnionT),
		g: union_.BuilderAdvanced(UnionChildrenBuilders, .{ .is_nullable = true, .is_dense = false }, void),
		h: dict.Builder(?u8),
	};
	var b = try struct_.BuilderAdvanced(T, .{ .is_nullable = true }, void).init(allocator);
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

	var a = try b.finish();
	{
		errdefer a.deinit();

		try a.toRecordBatch("table 1");
		out_array.* = try a.toOwnedAbi();
		out_schema.* = try a.ownedSchema();
	}
}

export fn sampleRecordBatch(out_array: *abi.Array, out_schema: *abi.Schema) callconv(.C) i64 {
	sampleRecordBatch2(std.heap.page_allocator, out_array, out_schema) catch return 1;
	return 0;
}

test {
	_ = @import("./abi.zig");
	_ = @import("./tags.zig");
	_ = @import("./array/array.zig");
	_ = @import("./array/flat.zig");
	_ = @import("./array/list.zig");
	_ = @import("./array/list_fixed.zig");
	_ = @import("./array/struct.zig");
	_ = @import("./array/union.zig");
	_ = @import("./array/dict.zig");
}

test "abi" {
	var arr: abi.Array = undefined;
	var schema: abi.Schema = undefined;
	try sampleRecordBatch2(std.testing.allocator, &arr, &schema);
	defer arr.release.?(&arr);
	defer schema.release.?(&schema);
	
	try std.testing.expectEqual(@as(i64, 8), schema.n_children);
}
