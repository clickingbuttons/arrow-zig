const std = @import("std");

// https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions
pub const Schema = extern struct {
	format: [*]const u8, // Managed
	name: [*]const u8 = &[_]u8{0}, // Managed
	metadata: [*]const u8 = &[_]u8{0}, // Managed
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
	buffers: ?[*]?*const anyopaque = null, // Managed
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

		const Self = @This();
		pub fn hasValidity(self: Self) bool {
			return switch (self) {
				.Primitive, .VariableBinary, .List, .FixedList, .Struct => true,
				else => false
			};
		}

		pub fn hasOffsets(self: Self) bool {
			return switch (self) {
				.VariableBinary, .List, .DenseUnion => true,
				else => false
			};
		}

		pub fn hasItems(self: Self) bool {
			return switch (self) {
				.Primitive, .VariableBinary => true,
				else => false
			};
		}
	};
};

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

