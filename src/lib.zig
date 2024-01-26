const std = @import("std");
pub const ffi = @import("./ffi/lib.zig");
pub const abi = ffi.abi;
pub const sample_arrays = @import("./sample_arrays.zig");
pub const ipc = @import("./ipc/lib.zig");
pub const Array = @import("./array/array.zig").Array;
pub const array = @import("./array/lib.zig");

fn sampleRecordBatch2(
    allocator: std.mem.Allocator,
    out_array: *abi.Array,
    out_schema: *abi.Schema,
) !void {
    var a = try sample_arrays.all(allocator);
    errdefer a.deinit();

    try a.toRecordBatch("table 1");
    out_array.* = try abi.Array.init(a);
    out_schema.* = try abi.Schema.init(a);
}

export fn sampleRecordBatch(out_array: *abi.Array, out_schema: *abi.Schema) callconv(.C) i64 {
    sampleRecordBatch2(std.heap.page_allocator, out_array, out_schema) catch return 1;
    return 0;
}

test {
    _ = @import("ffi/abi.zig");
    _ = @import("ffi/tests.zig");
    _ = @import("tags.zig");
    _ = @import("array/lib.zig");
    _ = @import("sample_arrays.zig");
    _ = @import("ipc/reader.zig");
    _ = @import("ipc/writer.zig");
}

test "abi doesn't leak" {
    var arr: abi.Array = undefined;
    var schema: abi.Schema = undefined;
    try sampleRecordBatch2(std.testing.allocator, &arr, &schema);
    defer arr.release.?(&arr);
    defer schema.release.?(&schema);

    {
        const sampleArr = try sample_arrays.all(std.testing.allocator);
        defer sampleArr.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(sampleArr.children.len)), schema.n_children);
    }
}
