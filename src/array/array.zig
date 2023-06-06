const std = @import("std");
const tags = @import("../tags.zig");
const abi = @import("../ffi/abi.zig");

const RecordBatchError = error {
	NotStruct,
};
const Allocator = std.mem.Allocator;
pub const BufferAlignment = abi.Array.BufferAlignment;

// This exists to be able to nest arrays at runtime.
pub const Array = struct {
	const Self = @This();
	pub const Bufs = [3][]align(BufferAlignment) u8;

	tag: tags.Tag,
	name: []const u8,
	allocator: Allocator,
	// TODO: remove this field, compute from tag
	length: usize,
	null_count: usize,
	// https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
	// Depending on layout stores validity, type_ids, offets, data, or indices.
	// You can tell how many buffers there are by looking at `tag.abiLayout().nBuffers()`
	bufs: Bufs,
	children: []*Array,

	pub fn init(allocator: Allocator) !*Self {
		return try allocator.create(Self);
	}

	pub fn deinit2(self: *Self, comptime free_children: bool) void {
		if (free_children) for (self.children) |c| c.deinit();
		if (self.children.len > 0) self.allocator.free(self.children);

		for (self.bufs) |b| if (b.len > 0) self.allocator.free(b);
		self.allocator.destroy(self);
	}

	pub fn deinit(self: *Self) void {
		self.deinit2(true);
	}

	pub fn toRecordBatch(self: *Self, name: []const u8) RecordBatchError!void {
		if (self.tag != .Struct) return RecordBatchError.NotStruct;
		// Record batches don't support nulls. It's ok to erase this because our struct impl saves null
		// info in the children arrays.
		// https://docs.rs/arrow-array/latest/arrow_array/array/struct.StructArray.html#comparison-with-recordbatch
		self.name = name;
		self.null_count = 0;
		self.tag.Struct.nullable = false;
		self.allocator.free(self.bufs[0]); // Free some memory.
		self.bufs[0].len = 0; // Avoid double free.
	}

	fn print2(self: *Self, depth: u8) void {
		const tab = (" " ** std.math.maxInt(u8))[0..depth*2];
		std.debug.print("{s}Array \"{s}\": {any}\n", .{ tab, self.name, self.tag });
		std.debug.print("{s}  null_count: {d} / {d}\n", .{ tab, self.null_count, self.length });
		for (self.bufs, 0..) |b, i| {
			std.debug.print("{s}  buf{d}: {any}\n", .{ tab, i, b });
		}
		for (self.children) |c| {
			c.print2(depth + 1);
		}
	}

	pub fn print(self: *Self) void {
		self.print2(0);
	}
};

const MaskInt = std.bit_set.DynamicBitSet.MaskInt;

fn numMasks(comptime T: type, bit_length: usize) usize {
	return (bit_length + (@bitSizeOf(T) - 1)) / @bitSizeOf(T);
}

pub fn validity(
	allocator: Allocator,
	bit_set: *std.bit_set.DynamicBitSet,
	null_count: usize
) ![]align(BufferAlignment) u8 {
	// Have to copy out for alignment until aligned bit masks land in std :(
	// https://github.com/ziglang/zig/issues/15600
	if (null_count == 0) {
		bit_set.deinit();
		return &.{};
	}
	const n_masks = numMasks(MaskInt, bit_set.unmanaged.bit_length);
	const n_mask_bytes = numMasks(u8, bit_set.unmanaged.bit_length);

	const copy = try allocator.alignedAlloc(u8, BufferAlignment, n_mask_bytes);
	const maskInts = bit_set.unmanaged.masks[0..n_masks];
	@memcpy(copy, std.mem.sliceAsBytes(maskInts)[0..n_mask_bytes]);
	bit_set.deinit();

	return copy;
}

// Dummy allocator
fn alloc(_: *anyopaque, _: usize, _: u8, _: usize) ?[*]u8 { return null; }
fn resize(_: *anyopaque, _: []u8, _: u8, _: usize, _: usize) bool { return false; }
fn free(_: *anyopaque, _: []u8, _: u8, _: usize) void {}

pub const null_array = Array {
	.tag = .Null,
	.name = &.{},
	.allocator = Allocator {
		.ptr = undefined,
		.vtable = &Allocator.VTable {
			.alloc = alloc,
			.resize = resize,
			.free = free,
		}
	},
	.length = 0,
	.null_count = 0,
	.bufs = .{ &.{}, &.{}, &.{} },
	.children = &.{},
};

test "null array" {
	var n = null_array;
	try std.testing.expectEqual(@as(usize, 0), n.null_count);
}

test "null array abi" {
	var n = null_array;
	var c = try abi.Array.init(&n);
	defer c.release.?(&c);
	try std.testing.expectEqual(@as(i64, 0), c.null_count);

	var s = try abi.Schema.init(&n);
	defer s.release.?(&s);
	try std.testing.expectEqualStrings("n\x00", s.format[0..2]);
	try std.testing.expectEqual(@as(i64, 0), s.n_children);
}
