const std = @import("std");
const abi = @import("./abi.zig");
const flat = @import("./array/flat.zig");

const TypeTag = enum {
	null,
	bool,
	i64,
	i32,
	i16,
	i8,
	u64,
	u32,
	u16,
	u8,
	f64,
	f32,
	f16,
	// timestamp,
	// date64,
	// date32,
	// time32,
	// time64,
	// duration,
	// interval,
	binary,
	// binary_fixed,
	binary_large,
	utf8,
	utf8_large,
	list,
	// list_fixed,
	// list_large,
	// struct,
	// union,
	// dict,
	// d128,
	// d256,
	// map,
	// run_end_encoded,
};

// I would love to use the zig type system, but zig's types must resolve at `comptime`. We cannot
// possibly enumerate all the nested types that may come in at runtime. So let's enumerate them.
pub const Tag = union(TypeTag) {
	null,
	bool,
	i64,
	i32,
	i16,
	i8,
	u64,
	u32,
	u16,
	u8,
	f64,
	f32,
	f16,
	// Timestamp(TimeUnit, Option<Arc<str>>),
	// date64,
	// date32,
	// Time32(TimeUnit),
	// Time64(TimeUnit),
	// Duration(TimeUnit),
	// Interval(IntervalUnit),
	binary,
	binary_large,
	// FixedSizeBinary(i32),
	utf8,
	utf8_large,
	list,
	// FixedSizeList(FieldRef, i32),
	// LargeList(FieldRef),
	// Struct(Fields),
	// Union(UnionFields, UnionMode),
	// Dictionary(Box<DataType>, Box<DataType>),
	// Decimal128(u8, i8),
	// Decimal256(u8, i8),
	// Map(FieldRef, bool),
	// RunEndEncoded(FieldRef, FieldRef),

	const Self = @This();
	pub fn fromType(comptime T: type, comptime is_large: bool, comptime is_utf8: bool) Self {
		// https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
		return switch (@typeInfo(T)) {
			.Void, .Null => .null,
			.Bool => .bool,
			.Int => |info| switch (info.bits) {
				64 => switch (info.signedness) {
					.signed => .i64,
					.unsigned => .u64,
				},
				32 => switch (info.signedness) {
					.signed => .i32,
					.unsigned => .u32,
				},
				16 => switch (info.signedness) {
					.signed => .i16,
					.unsigned => .u16,
				},
				8 => switch (info.signedness) {
					.signed => .i8,
					.unsigned => .u8,
				},
				else => |w| @compileError(std.fmt.comptimePrint("unsupported int width {}", .{w})),
			},
			.Float => |info| switch (info.bits) {
				64 => .f64,
				32 => .f32,
				16 => .f16,
				else => |w| @compileError(std.fmt.comptimePrint("unsupported float width {}", .{w})),
			},
			.Pointer => |p| switch (p.size) {
				.Slice => switch (p.child) {
					u8, ?u8 => switch (is_utf8) {
						true => if (comptime is_large) .utf8_large else .utf8,
						false => if (comptime is_large) .binary_large else .binary,
					},
					else => .list
				},
				else => @compileError("unsupported abi type " ++ @typeName(T))
			},
			// .Array => |a| {
			// 	const byte_size = a.len * @sizeOf(a.child);
			// 	return std.fmt.comptimePrint("w:{d}", .{byte_size});
			// },
			.Optional => |o| fromType(o.child, is_large, is_utf8),
			else => @compileError("unsupported abi type " ++ @typeName(T)),
		};
	}

	test "tag types" {
		try std.testing.expectEqual(Tag.u8, Tag.fromType(u8, false, false));
		try std.testing.expectEqual(Tag.i32, Tag.fromType(?i32, false, false));
		try std.testing.expectEqual(Tag.binary, Tag.fromType([]u8, false, false));
		try std.testing.expectEqual(Tag.binary, Tag.fromType([]?u8, false, false));
		try std.testing.expectEqual(Tag.binary_large, Tag.fromType([]u8, true, false));
		try std.testing.expectEqual(Tag.utf8, Tag.fromType([]u8, false, true));
		try std.testing.expectEqual(Tag.utf8_large, Tag.fromType([]u8, true, true));
		try std.testing.expectEqual(Tag.list, Tag.fromType([]i8, false, false));
	}

	pub fn ValueType(comptime self: Self) type {
		return switch (self) {
			.null, .list => void,
			.bool => bool,
			.i64 => i64,
			.i32 => i32,
			.i16 => i16,
			.i8 => i8,
			.u64 => u64,
			.u32 => u32,
			.u16 => u16,
			.u8, .binary, .binary_large, .utf8, .utf8_large => u8,
			.f64 => f64,
			.f32 => f32,
			.f16 => f16,
		};
	}

	pub fn abiLayout(self: Self) abi.Array.Layout {
		// https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
		// https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
		return switch (self) {
			.bool, .i64, .i32, .i16, .i8, .u64, .u32, .u16, .u8, .f64, .f32, .f16, => .Primitive,
			.null => .Null,
			.binary, .binary_large, .utf8, .utf8_large => .VariableBinary,
			.list => .List,
		};
	}

	pub fn abiFormat(self: Self) []const u8 {
		return switch (self) {
			.null => "n",
			.bool => "b",
			.i64 => "l",
			.u64 => "L",
			.i32 => "i",
			.u32 => "I",
			.i16 => "s",
			.u16 => "S",
			.i8 => "c",
			.u8 => "C",
			.f64 => "g",
			.f32 => "f",
			.f16 => "e",
			.binary => "z",
			.binary_large => "Z",
			.utf8 => "u",
			.utf8_large => "U",
			.list => "+l"
		};
	}
};

pub const Array = union(TypeTag) {
	null,
	bool: flat.Array(bool, false, false),
	i64: flat.Array(i64, false, false),
	i32: flat.Array(i32, false, false),
	i16: flat.Array(i16, false, false),
	i8: flat.Array(i8, false, false),
	u64: flat.Array(u64, false, false),
	u32: flat.Array(u32, false, false),
	u16: flat.Array(u16, false, false),
	u8: flat.Array(u8, false, false),
	f64: flat.Array(f64, false, false),
	f32: flat.Array(f32, false, false),
	f16: flat.Array(f16, false, false),
	// Timestamp(TimeUnit, Option<Arc<str>>),
	// date64,
	// date32,
	// Time32(TimeUnit),
	// Time64(TimeUnit),
	// Duration(TimeUnit),
	// Interval(IntervalUnit),
	binary: flat.Array([]u8, false, false),
	// FixedSizeBinary(i32),
	binary_large: flat.Array([]u8, true, false),
	utf8: flat.Array([]u8, false, true),
	utf8_large: flat.Array([]u8, true, true),
	list,

	pub fn init(arr: anytype) @This() {
		return @unionInit(@This(), @tagName(arr.tag), arr);
	}

	pub fn nullCount(self: @This()) i64 {
		return switch (self) {
			.null, .list => 0,
			inline else => |case| case.nullCount(),
		};
	}
};
