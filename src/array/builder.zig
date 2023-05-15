// Covenience builder for any non-dict array type
const flat = @import("./flat.zig");
const list = @import("./list.zig");
const list_fixed = @import("./list_fixed.zig");
const struct_ = @import("./struct.zig");
const union_ = @import("./union.zig");
const dict = @import("./dict.zig");

fn Builder2(comptime ctx: type, comptime T: type) type {
	return switch (@typeInfo(ctx)) {
		.Bool, .Int, .Float => flat.Builder(T),
		.Pointer => |p| switch (p.size) {
			.Slice => switch (p.child) {
				u8, ?u8 => flat.Builder(T),
				else => list.Builder(T),
			},
			else => @compileError("unsupported builder type " ++ @typeName(T))
		},
		.Array => list_fixed.Builder(T),
		.Optional => |o| Builder2(o.child, T),
		.Struct => struct_.Builder(T),
		.Union => union_.Builder(T),
		else => @compileError("unsupported builder type " ++ @typeName(T))
	};
}

pub fn Builder(comptime T: type) type {
	return Builder2(T, T);
}
