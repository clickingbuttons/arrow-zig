const std = @import("std");

pub const BufferAlignment = 64;

// https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions
pub const Schema = extern struct {
	format: [*:0]const u8, // Managed
	name: ?[*:0]const u8 = null, // Managed
	metadata: ?[*:0]const u8 = null, // Managed
	flags: packed struct(i64) {
		dictionary_ordered: bool = false,
		nullable: bool = false,
		map_keys_sorted: bool = false,
		_padding: u61 = 0,
	} = .{},
	n_children: i64 = 0,
	children: ?[*]*Schema = null, // Managed
	dictionary: ?*Schema = null, // Managed
	release: ?*const fn (*Schema) callconv(.C) void = null,
	private_data: ?*anyopaque = null,

	comptime {
		std.debug.assert(@sizeOf(@This()) == 72);
	}
};

pub const Array = extern struct {
	length: i64 = 0,
	null_count: i64 = 0,
	offset: i64 = 0,
	n_buffers: i64,
	n_children: i64 = 0,
	buffers: ?[*]?*align(BufferAlignment) const anyopaque = null, // Managed
	children: ?[*]*Array = null, // Managed
	dictionary: ?*Array = null, // Managed
	release: ?*const fn (*Array) callconv(.C) void = null,
	private_data: ?*anyopaque = null,

	comptime {
		std.debug.assert(@sizeOf(@This()) == 80);
	}

	// https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
	pub const Layout = enum {
		Primitive,
		VariableBinary,
		List,
		FixedList,
		Struct,
		SparseUnion,
		DenseUnion,
		Null,
		Dictionary,
		Map, // Undocumented :)

		const Self = @This();
		pub fn hasTypeIds(self: Self) bool {
			return switch (self) {
				.SparseUnion, .DenseUnion => true,
				else => false
			};
		}

		pub fn hasValidity(self: Self) bool {
			return switch (self) {
				.Primitive, .VariableBinary, .List, .FixedList, .Struct, .Dictionary, .Map => true,
				else => false
			};
		}

		pub fn hasOffsets(self: Self) bool {
			return switch (self) {
				.VariableBinary, .List, .DenseUnion, .Map => true,
				else => false
			};
		}

		pub fn hasValues(self: Self) bool {
			return switch (self) {
				.Primitive, .VariableBinary, .Dictionary => true,
				else => false
			};
		}

		pub fn nBuffers(self: Self) usize {
			const res =
				@intCast(usize, @boolToInt(self.hasTypeIds())) +
				@intCast(usize, @boolToInt(self.hasValidity())) +
				@intCast(usize, @boolToInt(self.hasOffsets())) +
				@intCast(usize, @boolToInt(self.hasValues()));
			std.debug.assert(res <= 3);
			return res;
		}

		pub fn hasChildren(self: Self) bool {
			return switch (self) {
				.List, .FixedList, .Struct, .SparseUnion, .DenseUnion, .Dictionary, .Map => true,
				else => false,
			};
		}
	};
};

fn testLayout(layout: Array.Layout, n_buffers: usize) !void {
	try std.testing.expectEqual(n_buffers, layout.nBuffers());
}
test "nbuffers" {
	// https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
	try testLayout(Array.Layout.Primitive, 2);
	try testLayout(Array.Layout.VariableBinary, 3);
	try testLayout(Array.Layout.List, 2);
	try testLayout(Array.Layout.FixedList, 1);
	try testLayout(Array.Layout.Struct, 1);
	try testLayout(Array.Layout.SparseUnion, 1);
	try testLayout(Array.Layout.DenseUnion, 2);
	try testLayout(Array.Layout.Null, 0);
	try testLayout(Array.Layout.Dictionary, 2);
}

pub const ArrayStream = extern struct {
	get_schema: *const fn (*ArrayStream, *Schema) callconv(.C) c_int,
	get_next: *const fn (*ArrayStream, *Array) callconv(.C) c_int,
	get_last_error: *const fn (*ArrayStream) callconv(.C) [*]const u8,
	release: *const fn (*ArrayStream) callconv(.C) void,
	private_data: ?*anyopaque,

	comptime {
		std.debug.assert(@sizeOf(@This()) == 40);
	}
};

