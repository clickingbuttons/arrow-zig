const std = @import("std");
const abi = @import("abi.zig");
const Array = @import("../array/array.zig").Array;

const Allocator = std.mem.Allocator;
const Buffer = abi.Array.Buffer;
const Buffers = abi.Array.Buffers;

pub fn buffers(array: *Array, n_buffers: usize) !Buffers {
	if (n_buffers == 0) return null;

	const res = try array.allocator.alloc(Buffer, n_buffers);
	for (0..n_buffers) |i| {
		const b = array.buffers[i];
		res[i] = if (b.len > 0) @ptrCast(Buffer, b.ptr) else null;
	}

	return @ptrCast(Buffers, res);
}

pub fn children(array: *Array, n_children: usize) Allocator.Error!?[*]*abi.Array {
	if (n_children == 0) return null;

	const res = try array.allocator.alloc(*abi.Array, n_children);
	for (0..n_children) |j| {
		res[j] = try array.allocator.create(abi.Array);
		res[j].* = try abi.Array.init(array.children[j]);
	}

	return @ptrCast(?[*]*abi.Array, res);
}

pub fn dictionary(array: *Array, layout: abi.Array.Layout) Allocator.Error!?*abi.Array {
	if (layout != .Dictionary) return null;

	var res = try array.allocator.create(abi.Array);
	res.* = try abi.Array.init(array.children[0]);

	return @ptrCast(?*abi.Array, res);
}

pub fn release(array: *abi.Array) callconv(.C) void {
	const allocator = brk: {
		const arr = @ptrCast(*Array, @alignCast(@alignOf(Array), array.private_data));
		const res = arr.allocator;
		arr.deinit2(false);
		break :brk res;
	};

	if (array.buffers) |b| allocator.free(b[0..@intCast(usize, array.n_buffers)]);
	if (array.children) |children_| {
		const slice = children_[0..@intCast(usize, array.n_children)];
		for (slice) |child| {
			if (child.release) |r| r(child);
			allocator.destroy(child);
		}
		allocator.free(slice);
	}
	if (array.dictionary) |dictionary_| {
		if (dictionary_.release) |r| r(dictionary_);
		allocator.destroy(dictionary_);
	}
	array.*.release = null;
}
