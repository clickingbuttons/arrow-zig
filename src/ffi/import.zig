const std = @import("std");
const abi = @import("abi.zig");
const tags = @import("../tags.zig");
const Array = @import("../array/array.zig").Array;

const log = std.log.scoped(.arrow);
const Allocator = std.mem.Allocator;

const ImportError = error {
	TooManyBuffers,
	SchemaLenMismatch,
	BufferLenMismatch,
};

pub const ImportedArray = struct {
	const Self = @This();

	array: *Array,

	// We have to store this arr and schema for our whole lifetime so we can call their release functions.
	// The alternative is to copy the schema and array data out but that's slower for large arrays
	abi_schema: abi.Schema,
	abi_arr: abi.Array,

	fn importBuf(buffers: [*]abi.Array.Buffer, index: usize, len: usize) Array.Buffer {
		if (buffers[index]) |buf| return @constCast(buf[0..len]);
		return &.{};
	}

	fn importBufs(arr: *const abi.Array, tag: tags.Tag, arr_len: usize) !Array.Buffers {
		const abi_layout = tag.abiLayout();
		const n_buffers = abi_layout.nBuffers();
		const actual_buffers = @intCast(usize, arr.n_buffers);
		if (n_buffers != actual_buffers) {
			log.err("expected {d} buffers for tag {any}, got {d}", .{ n_buffers, tag, actual_buffers });
			return ImportError.BufferLenMismatch;
		}

		var res = Array.Buffers { &.{}, &.{}, &.{} };
		if (n_buffers == 0) return res;

		if (arr.buffers == null) {
			log.err("expected {d} buffers for tag {any} but buffer list is null", .{ n_buffers, tag });
			return ImportError.BufferLenMismatch;
		}

		const arr_buffers = arr.buffers.?;
		var validity_len = arr_len / 8;
		if (arr_len % 8 > 0) validity_len += 1;

		switch (abi_layout) {
			.Primitive => {
				res[0] = importBuf(arr_buffers, 0, validity_len);
				res[1] = importBuf(arr_buffers, 1, arr_len * tag.size());
			},
			.VariableBinary => {
				res[0] = importBuf(arr_buffers, 0, validity_len);
				res[1] = importBuf(arr_buffers, 1, arr_len * tag.offsetSize());
				res[2] = importBuf(arr_buffers, 2, arr_len * tag.size());
			},
			.List => {
				res[0] = importBuf(arr_buffers, 0, validity_len);
				res[1] = importBuf(arr_buffers, 1, arr_len * tag.offsetSize());
			},
			.FixedList, .Struct => {
				res[0] = importBuf(arr_buffers, 0, validity_len);
			},
			.SparseUnion => {
				res[0] = importBuf(arr_buffers, 0, arr_len);
			},
			.DenseUnion => {
				res[0] = importBuf(arr_buffers, 0, arr_len);
				res[1] = importBuf(arr_buffers, 1, arr_len * tag.offsetSize());
			},
			.Null => {},
			.Dictionary => {
				res[0] = importBuf(arr_buffers, 0, validity_len);
				res[1] = importBuf(arr_buffers, 0, arr_len * tag.size());
			},
			.Map => { // Undocumented :)
				res[0] = importBuf(arr_buffers, 0, validity_len);
				res[1] = importBuf(arr_buffers, 0, arr_len * @sizeOf(i32));
			},
		}

		return res;
	}

	fn initArray(allocator: Allocator, arr: *const abi.Array, schema: *const abi.Schema) !*Array {
		const format = std.mem.span(schema.format);
		const tag = try tags.Tag.fromAbiFormat(format, schema.flags.nullable, schema.dictionary != null);

		const arr_len = @intCast(usize, arr.length);
		const buffers = try importBufs(arr, tag, arr_len);

		if (arr.n_children != schema.n_children) return ImportError.SchemaLenMismatch;
		const n_children = @intCast(usize, arr.n_children);
		var children = try allocator.alloc(*Array, n_children);
		var i: usize = 0;
		errdefer {
			for (0..i) |j| allocator.destroy(children[j]);
			allocator.free(children);
		}
		while (i < n_children) : (i += 1) {
			children[i] = try initArray(allocator, arr.children.?[i], schema.children.?[i]);
		}

		var res = try allocator.create(Array);
		res.* = .{
			.tag = tag,
			.name = if (schema.name) |n| std.mem.span(n) else "imported array",
			.allocator = allocator,
			.length = arr_len,
			.null_count = @intCast(usize, arr.null_count),
			.buffers = buffers,
			.children = children,
		};

		return res;
	}

	pub fn init(allocator: Allocator, arr: abi.Array, schema: abi.Schema) !Self {
		return .{
			.array = try initArray(allocator, &arr, &schema),
			.abi_schema = schema,
			.abi_arr = arr,
		};
	}

	fn deinitArray(allocator: Allocator, array: *Array) void {
		for (array.children) |child| deinitArray(allocator, child);
		allocator.free(array.children);
		allocator.destroy(array);
	}

	pub fn deinit(self: *Self) void {
		const allocator = self.array.allocator;
		deinitArray(allocator, self.array);
		if (self.abi_schema.release) |r| r(&self.abi_schema);
		if (self.abi_arr.release) |r| r(&self.abi_arr);
	}
};

