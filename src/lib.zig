const std = @import("std");
const testing = std.testing;

export fn add(a: i32, b: i32) i32 {
    return a + b;
}

test {
	_ = @import("./abi.zig");
	_ = @import("./tags.zig");
	_ = @import("./array/flat.zig");
	_ = @import("./array/list.zig");
	_ = @import("./array/list_fixed.zig");
}
