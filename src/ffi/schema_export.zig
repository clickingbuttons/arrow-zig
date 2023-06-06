const std = @import("std");
const abi = @import("abi.zig");
const Array = @import("../array/array.zig").Array;
const Allocator = std.mem.Allocator;

pub fn children(arr: *Array, n_children: usize) Allocator.Error!?[*]*abi.Schema {
	if (n_children == 0) return null;

	const res = try arr.allocator.alloc(*abi.Schema, n_children);
	for (0..n_children) |j| {
		res[j] = try arr.allocator.create(abi.Schema);
		res[j].* = try abi.Schema.init(arr.children[j]);
	}
	return @ptrCast(?[*]*abi.Schema, res);
}

pub fn dictionary(arr: *Array, layout: abi.Array.Layout) Allocator.Error!?*abi.Schema {
	if (layout != .Dictionary) return null;

	var res = try arr.allocator.create(abi.Schema);
	res.* = try abi.Schema.init(arr.children[0]);
	return @ptrCast(?*abi.Schema, res);
}

pub fn release(schema: *abi.Schema) callconv(.C) void {
	const arr = @ptrCast(*Array, @alignCast(@alignOf(Array), schema.private_data));
	const allocator = arr.allocator;
	if (schema.children) |c| {
		for (0..@intCast(usize, schema.n_children)) |i| {
			c[i].release.?(c[i]);
			allocator.destroy(c[i]);
		}
		allocator.free(c[0..@intCast(usize, schema.n_children)]);
	}
	if (schema.dictionary) |d| {
		d.release.?(d);
		allocator.destroy(d);
	}
	if (schema.name) |n| allocator.free(n[0..arr.name.len + 1]);
	if (arr.tag.isAbiFormatOnHeap()) {
		allocator.free(std.mem.span(schema.format));
	}
	schema.*.release = null;
}
