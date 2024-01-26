const std = @import("std");
const arrow = @import("arrow");

const abi = arrow.ffi.abi;
const allocator = std.testing.allocator;

test "ffi export" {
    const array = try arrow.sample_arrays.all(allocator);
    errdefer array.deinit();

    // Note: these are stack allocated.
    var abi_arr = try abi.Array.init(array);
    var abi_schema = try abi.Schema.init(array);

    // externFn(&abi_schema, &abi_arr);

    // Normally `externFn` would call these. The order doesn't matter.
    abi_schema.release.?(&abi_schema);
    abi_arr.release.?(&abi_arr);
}

test "ffi import" {
    const array = try arrow.sample_arrays.all(allocator);

    var abi_schema = try abi.Schema.init(array);
    var abi_arr = try abi.Array.init(array);
    var imported = try arrow.ffi.ImportedArray.init(allocator, abi_arr, abi_schema);
    defer imported.deinit();
}
