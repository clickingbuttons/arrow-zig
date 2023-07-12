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
    return @ptrCast(res);
}

pub fn dictionary(arr: *Array, layout: abi.Array.Layout) Allocator.Error!?*abi.Schema {
    if (layout != .Dictionary) return null;

    var res = try arr.allocator.create(abi.Schema);
    res.* = try abi.Schema.init(arr.children[0]);
    return @ptrCast(res);
}

pub fn release(schema: *abi.Schema) callconv(.C) void {
    const arr: *align(1) Array = @ptrCast(schema.private_data);
    const allocator = arr.allocator;
    if (schema.children) |children_| {
        const len: usize = @bitCast(schema.n_children);
        const children_slice = children_[0..len];
        for (children_slice) |child| {
            if (child.release) |release_| release_(child);
            allocator.destroy(child);
        }
        allocator.free(children_slice);
    }
    if (schema.dictionary) |d| {
        d.release.?(d);
        allocator.destroy(d);
    }
    if (schema.name) |n| allocator.free(n[0 .. arr.name.len + 1]);
    if (arr.tag.isAbiFormatOnHeap()) allocator.free(std.mem.span(schema.format));
    schema.*.release = null;
}
