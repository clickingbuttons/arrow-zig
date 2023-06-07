const std = @import("std");
const flat = @import("./flat.zig");
const list = @import("./list.zig");
const struct_ = @import("./struct.zig");
const union_ = @import("./union.zig");
const dict = @import("./dict.zig");
const map = @import("./map.zig");

fn isMapLike(comptime T: type) bool {
	return switch (@typeInfo(T)) {
		.Struct => |s| s.is_tuple and s.fields.len == 2 and @typeInfo(s.fields[0].type) != .Optional,
		else => false
	};
}

test "is map like" {
	try std.testing.expectEqual(true, isMapLike(struct { []const u8, i32 }));
	try std.testing.expectEqual(true, isMapLike(struct { i32, ?i32 }));
	try std.testing.expectEqual(false, isMapLike(struct { ?i32, i32 }));
	try std.testing.expectEqual(false, isMapLike(struct { i32, i32, i32 }));
}

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
		.Array => |a| switch (a.child) {
			u8, ?u8 => flat.Builder(T),
			else => list.Builder(T),
		},
		.Optional => |o| Builder2(o.child, T),
		.Struct => if (isMapLike(T)) map.Builder(T) else struct_.Builder(T),
		.Union => union_.Builder(T),
		else => @compileError("unsupported builder type " ++ @typeName(T))
	};
}

// Covenience builder for any non-dict array type
pub fn Builder(comptime T: type) type {
	return Builder2(T, T);
}
