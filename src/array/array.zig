const std = @import("std");
const tags = @import("../tags.zig");
const abi = @import("../abi.zig");

pub const MaskInt = std.bit_set.DynamicBitSet.MaskInt;
// This exists to be able to nest arrays at runtime.
pub const Array = struct {
	tag: tags.Tag, // 1
	allocator: std.mem.Allocator, // 2
	null_count: i64, // 1
	// TODO: align(64)
	validity: []MaskInt, // 2
	offsets: []align(64) u8, // 2
	values: []align(64) u8, // 2
	children: []Array, // 2

	const Self = @This();

	fn arrayRelease(arr: *abi.Array) callconv(.C) void {
		const self = @ptrCast(*Self, @alignCast(@alignOf(Self), arr.private_data));
		self.deinit();
		if (arr.buffers) |buffers| {
			self.allocator.free(buffers[0..@intCast(usize, arr.n_buffers)]);
		}
		if (arr.children) |children| {
			for (0..@intCast(usize, arr.n_children)) |i| {
				self.allocator.destroy(children[i]);
			}
			self.allocator.free(children[0..@intCast(usize, arr.n_children)]);
		}
		if (arr.dictionary) |dictionary| {
			self.allocator.destroy(dictionary);
		}
		arr.*.release = null;
	}

	pub fn deinit(self: Self) void {
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

	pub fn values_as(self: Self, comptime T: type) []T {
		return std.mem.bytesAsSlice(T, self.values);
	}

	fn abiBuffers(self: Self, layout: abi.Array.Layout, n_buffers: usize) std.mem.Allocator.Error!?[*]?*const anyopaque {
		if (n_buffers == 0) {
			return null;
		}

		const buffers = try self.allocator.alloc(?*const anyopaque, n_buffers);
		@memset(buffers, null);
		var i: usize = 0;
		if (layout.hasTypeIds()) {
			if (self.values.len > 0) {
				buffers[i] = @ptrCast(?*const anyopaque, self.values.ptr);
			}
			i += 1;
		}
		if (layout.hasValidity()) {
			if (self.null_count > 0) {
				buffers[i] = @ptrCast(?*const anyopaque, self.validity.ptr);
			}
			i += 1;
		}
		if (layout.hasOffsets()) {
			if (self.offsets.len > 0) {
				buffers[i] = @ptrCast(?*const anyopaque, self.offsets.ptr);
			}
			i += 1;
		}
		if (layout.hasData()) {
			if (self.values.len > 0) {
				buffers[i] = @ptrCast(?*const anyopaque, self.values.ptr);
			}
			i += 1;
		}

		return @ptrCast(?[*]?*const anyopaque, buffers);
	}

	fn abiChildren(self: Self, n_children: usize) std.mem.Allocator.Error!?[*]*abi.Array {
		if (n_children == 0) {
			return null;
		}
		const children = try self.allocator.alloc(*abi.Array, n_children);
		for (0..n_children) |j| {
			children[j] = try self.allocator.create(abi.Array);
			children[j].* = try self.children[j].toOwnedAbi();
		}

		return @ptrCast(?[*]*abi.Array, children);
	}

	fn abiDictionary(self: Self, layout: abi.Array.Layout) std.mem.Allocator.Error!?*abi.Array {
		if (layout != .Dictionary) {
			return null;
		}

		var dictionary = try self.allocator.create(abi.Array);
		dictionary.* = try self.children[0].toOwnedAbi();

		return @ptrCast(?*abi.Array, dictionary);
	}

	pub fn toOwnedAbi(self: *Self) std.mem.Allocator.Error!abi.Array {
		const layout = self.tag.abiLayout();
		const n_buffers = layout.nBuffers();
		const n_children = if (layout == .Dictionary) 0 else self.children.len;

		return .{
			// .length = i64 = 0,
			.null_count = self.null_count,
			.offset = 0,
			.n_buffers = @intCast(i64, n_buffers),
			.n_children = @intCast(i64, n_children),
			.buffers = try self.abiBuffers(layout, n_buffers),
			.children = try self.abiChildren(n_children),
			.dictionary = try self.abiDictionary(layout),
			.release = arrayRelease,
			.private_data = @ptrCast(?*anyopaque, self),
		};
	}
};

fn numMasks(bit_length: usize) usize {
	return (bit_length + (@bitSizeOf(MaskInt) - 1)) / @bitSizeOf(MaskInt);
}

pub fn validity(bit_set: *std.bit_set.DynamicBitSet, null_count: i64) []MaskInt {
	if (null_count == 0) {
		bit_set.deinit();
		return &.{};
	}
	return bit_set.unmanaged.masks[0..numMasks(bit_set.unmanaged.bit_length)];
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
	.validity = &.{},
	.offsets = &.{},
	.values = &.{},
	.children = &.{},
};

test "null array" {
	const n = null_array;
	defer n.deinit();
	try std.testing.expectEqual(@as(i64, 0), n.null_count);
}

test "null array abi" {
	var n = null_array;
	const c = try n.toOwnedAbi();
	defer c.release.?(@constCast(&c));
	try std.testing.expectEqual(@as(i64, 0), c.null_count);
}
