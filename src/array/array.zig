const std = @import("std");
const tags = @import("../tags.zig");

pub const MaskInt = std.bit_set.DynamicBitSet.MaskInt;
pub const Array = struct {
	tag: tags.Tag, // 1
	allocator: std.mem.Allocator, // 2
	null_count: i64, // 1
	// TODO: align(64)
	validity: []MaskInt, // 2
	offsets: []align(64) u8, // 2
	values: []align(64) u8, // 2
	children: []Array, // 2

	pub fn deinit(self: @This()) void {
		for (self.children) |c| {
			c.deinit();
		}

		// See bit_set.zig#deinit
		if (self.validity.len > 0) {
			const old_allocation = (self.validity.ptr - 1)[0..(self.validity.ptr - 1)[0]];
			self.allocator.free(old_allocation);
		}

		if (self.offsets.len > 0) {
			self.allocator.free(self.offsets);
		}
		if (self.values.len > 0) {
			self.allocator.free(self.values);
		}
		if (self.children.len > 0) {
			self.allocator.free(self.children);
		}
	}

	pub fn values_as(self: @This(), comptime T: type) []T {
		return std.mem.bytesAsSlice(T, self.values);
	}
};

pub fn numMasks(bit_length: usize) usize {
	return (bit_length + (@bitSizeOf(MaskInt) - 1)) / @bitSizeOf(MaskInt);
}

// Dummy allocator
fn alloc(_: *anyopaque, _: usize, _: u8, _: usize) ?[*]u8 { return null; }
fn resize(_: *anyopaque, _: []u8, _: u8, _: usize, _: usize) bool { return false; }
fn free(_: *anyopaque, _: []u8, _: u8, _: usize) void {}

pub const null_array = Array {
	.tag = .null,
	.allocator = std.mem.Allocator {
		.ptr = undefined,
		.vtable = &std.mem.Allocator.VTable {
			.alloc = alloc,
			.resize = resize,
			.free = free,
		}
	},
	.null_count = 0,
	.validity = &[_]MaskInt{},
	.offsets = &[_]u8{},
	.values = &[_]u8{},
	.children = &[_]Array{},
};

test "null array" {
	const n = null_array;
	defer n.deinit();
	try std.testing.expectEqual(@as(i64, 0), n.null_count);
}
