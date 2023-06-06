const std = @import("std");
const abi = @import("abi.zig");
const Array = @import("../array/array.zig").Array;

const Allocator = std.mem.Allocator;
const BufferAlignment = Array.BufferAlignment;
const BufferPtrs = abi.Array.BufferPtrs;
const BufferPtr = abi.Array.BufferPtr;

pub fn buffers(arr: *Array, n_buffers: usize) !BufferPtrs {
	if (n_buffers == 0) return null;

	const res = try arr.allocator.alloc(BufferPtr, n_buffers);
	for (0..n_buffers) |i| {
		const b = arr.bufs[i];
		res[i] = if (b.len > 0) @ptrCast(BufferPtr, b.ptr) else null;
	}

	return @ptrCast(BufferPtrs, res);
}

pub fn children(arr: *Array, n_children: usize) Allocator.Error!?[*]*abi.Array {
	if (n_children == 0) return null;

	const res = try arr.allocator.alloc(*abi.Array, n_children);
	for (0..n_children) |j| {
		res[j] = try arr.allocator.create(abi.Array);
		res[j].* = try abi.Array.init(arr.children[j]);
	}

	return @ptrCast(?[*]*abi.Array, res);
}

pub fn dictionary(arr: *Array, layout: abi.Array.Layout) Allocator.Error!?*abi.Array {
	if (layout != .Dictionary) return null;

	var res = try arr.allocator.create(abi.Array);
	res.* = try abi.Array.init(arr.children[0]);

	return @ptrCast(?*abi.Array, res);
}

pub fn release(arr: *abi.Array) callconv(.C) void {
	const array = @ptrCast(*Array, @alignCast(@alignOf(Array), arr.private_data));
	const allocator = array.allocator;

	if (arr.buffers) |b| allocator.free(b[0..@intCast(usize, arr.n_buffers)]);
	if (arr.children) |c| {
		for (0..@intCast(usize, arr.n_children)) |i| {
			if (c[i].release) |r| r(c[i]);
			allocator.destroy(c[i]);
		}
		allocator.free(c[0..@intCast(usize, arr.n_children)]);
	}
	if (arr.dictionary) |d| {
		d.release.?(d);
		allocator.destroy(d);
	}

	array.deinit2(false);

	arr.*.release = null;
}
