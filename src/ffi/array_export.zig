const std = @import("std");
const abi = @import("abi.zig");
const Array = @import("../array/array.zig").Array;

const Allocator = std.mem.Allocator;
const Buffer = abi.Array.Buffer;
const Buffers = abi.Array.Buffers;

fn buffers(array: *Array, n_buffers: usize) Allocator.Error!Buffers {
    if (n_buffers == 0) return null;

    const res = try array.allocator.alloc(Buffer, n_buffers);
    for (array.buffers[0..n_buffers], 0..) |b, i| {
        res[i] = if (b.len > 0) @ptrCast(b.ptr) else null;
    }

    return @ptrCast(res);
}

fn children(array: *Array, n_children: usize) Allocator.Error!?[*]*abi.Array {
    if (array.children.len == 0) return null;

    const allocator = array.allocator;
    const res = try allocator.alloc(*abi.Array, n_children);
    var i: usize = 0;
    // TODO: how to track if allocator.create fails?
    errdefer {
        for (0..i) |j| {
            res[j].deinit();
            allocator.destroy(res[j]);
        }
    }
    for (0..n_children) |_| {
        res[i] = try array.allocator.create(abi.Array);
        res[i].* = try abi.Array.init(array.children[i]);
        i += 1;
    }

    return @ptrCast(res);
}

fn dictionary(array: *Array, layout: abi.Array.Layout) Allocator.Error!?*abi.Array {
    if (layout != .Dictionary) return null;

    const allocator = array.allocator;
    var res = try allocator.create(abi.Array);
    errdefer allocator.destroy(res);
    res.* = try abi.Array.init(array.children[0]);

    return @ptrCast(res);
}

pub fn init(array: *Array) Allocator.Error!abi.Array {
    const layout = array.tag.abiLayout();
    const n_buffers = layout.nBuffers();
    const n_children = if (layout == .Dictionary) 0 else array.children.len;
    const allocator = array.allocator;

    const buffers_ = try buffers(array, n_buffers);
    errdefer if (buffers_) |buffers__| allocator.free(buffers__[0..n_buffers]);
    const children_ = try children(array, n_children);
    errdefer if (buffers_) |buffers__| allocator.free(buffers__[0..n_buffers]);
    const dictionary_ = try dictionary(array, layout);
    errdefer if (dictionary_) |d| if (d.release) |r| r(d);

    return .{
        .length = @bitCast(array.length),
        .null_count = @bitCast(array.null_count),
        .offset = 0,
        .n_buffers = @bitCast(n_buffers),
        .n_children = @bitCast(n_children),
        .buffers = buffers_,
        .children = children_,
        .dictionary = dictionary_,
        .release = release,
        .private_data = @ptrCast(array),
    };
}

pub fn release(array: *abi.Array) callconv(.C) void {
    const allocator = brk: {
        const arr: *Array = @ptrCast(@alignCast(array.private_data));
        const res = arr.allocator;
        arr.deinitAdvanced(false, true);
        break :brk res;
    };

    if (array.buffers) |b| allocator.free(b[0..@intCast(array.n_buffers)]);
    if (array.children) |children_| {
        const slice = children_[0..@intCast(array.n_children)];
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
