const std = @import("std");
const abi = @import("abi.zig");
const Array = @import("../array/array.zig").Array;
const Allocator = std.mem.Allocator;

fn children(arr: *Array, n_children: usize) Allocator.Error!?[*]*abi.Schema {
    if (n_children == 0) return null;

    const allocator = arr.allocator;
    const res = try allocator.alloc(*abi.Schema, n_children);
    var i: usize = 0;
    // TODO: how to track if allocator.create fails?
    errdefer {
        for (0..i) |j| {
            res[j].deinit();
            allocator.destroy(res[j]);
        }
    }
    for (0..n_children) |_| {
        res[i] = try allocator.create(abi.Schema);
        res[i].* = try abi.Schema.init(arr.children[i]);
        i += 1;
    }
    return @ptrCast(res);
}

fn dictionary(arr: *Array, layout: abi.Array.Layout) Allocator.Error!?*abi.Schema {
    if (layout != .Dictionary) return null;

    const allocator = arr.allocator;
    var res = try allocator.create(abi.Schema);
    errdefer allocator.destroy(res);
    res.* = try abi.Schema.init(arr.children[0]);
    return @ptrCast(res);
}

fn privateData(arr: *Array, format_on_heap: bool) Allocator.Error!?*abi.Schema.PrivateData {
    if (arr.name.len == 0 and !format_on_heap) return null;

    const allocator = arr.allocator;
    var res = try allocator.create(abi.Schema.PrivateData);
    errdefer allocator.destroy(res);
    res.* = .{
        .allocator = allocator,
        .name_len = arr.name.len,
        .abi_format_on_heap = format_on_heap,
    };
    return res;
}

pub fn init(array: *Array) Allocator.Error!abi.Schema {
    const allocator = array.allocator;
    const layout = array.tag.abiLayout();
    const n_children = if (layout == .Dictionary) 0 else array.children.len;
    const format_on_heap = array.tag.isAbiFormatOnHeap();

    const format = try array.tag.abiFormat(allocator, n_children);
    errdefer if (format_on_heap) allocator.free(std.mem.span(format));

    const name: ?[*:0]const u8 = if (array.name.len == 0)
        null
    else
        try allocator.dupeZ(u8, array.name);
    errdefer if (name) |n| allocator.free(n[0..array.name.len]);

    const children_ = try children(array, n_children);
    errdefer {
        if (children_) |children__| {
            for (children__[0..n_children]) |c| {
                c.deinit();
                allocator.destroy(c);
            }
        }
    }

    const dictionary_ = try dictionary(array, layout);
    errdefer if (dictionary_) |d| if (d.release) |r| r(d);

    const private_data = try privateData(array, format_on_heap);
    errdefer if (private_data) |p| allocator.destroy(p);

    return .{
        .format = format,
        .name = name,
        .metadata = null,
        .flags = .{ .nullable = array.tag.nullable() },
        .n_children = @bitCast(n_children),
        .children = children_,
        .dictionary = dictionary_,
        .release = release,
        .private_data = @ptrCast(private_data),
    };
}

pub fn release(schema: *abi.Schema) callconv(.C) void {
    if (schema.private_data == null) return;
    const private_data: *abi.Schema.PrivateData = @ptrCast(@alignCast(schema.private_data));
    const allocator = private_data.allocator;
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
    if (schema.name) |n| allocator.free(n[0 .. private_data.name_len + 1]);
    if (private_data.abi_format_on_heap) allocator.free(std.mem.span(schema.format));
    allocator.destroy(private_data);
    schema.*.release = null;
}
