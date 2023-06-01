const std = @import("std");
const abi = @import("./abi.zig");
const array = @import("./array/array.zig");
const sample = @import("./sample.zig");

fn sampleRecordBatch2(allocator: std.mem.Allocator, out_array: *abi.Array, out_schema: *abi.Schema) !void {
	var a = try sample.sampleArray(allocator);
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
	_ = @import("./array/struct.zig");
	_ = @import("./array/union.zig");
	_ = @import("./array/dict.zig");
	_ = @import("./array/map.zig");
	_ = @import("./sample.zig");
	_ = @import("./ipc/reader.zig");
}

test "abi doesn't leak" {
	var arr: abi.Array = undefined;
	var schema: abi.Schema = undefined;
	try sampleRecordBatch2(std.testing.allocator, &arr, &schema);
	defer arr.release.?(&arr);
	defer schema.release.?(&schema);

	{
		const sampleArr = try sample.sampleArray(std.testing.allocator);
		defer sampleArr.deinit();
		try std.testing.expectEqual(@intCast(i64, sampleArr.children.len), schema.n_children);
	}
}
