const std = @import("std");
const abi = @import("./abi.zig");
const flat = @import("./array/flat.zig");

pub const Tag = union(enum) {
	null,
	bool: struct { is_nullable: bool },
	i64: struct { is_nullable: bool },
	i32: struct { is_nullable: bool },
	i16: struct { is_nullable: bool },
	i8: struct { is_nullable: bool },
	u64: struct { is_nullable: bool },
	u32: struct { is_nullable: bool },
	u16: struct { is_nullable: bool },
	u8: struct { is_nullable: bool },
	f64: struct { is_nullable: bool },
	f32: struct { is_nullable: bool },
	f16: struct { is_nullable: bool },
	// Timestamp(TimeUnit, Option<Arc<str>>),
	// date64,
	// date32,
	// Time32(TimeUnit),
	// Time64(TimeUnit),
	// Duration(TimeUnit),
	// Interval(IntervalUnit),
	binary: struct { is_large: bool, is_utf8: bool },
	// FixedSizeBinary(i32),
	list: struct { is_nullable: bool, is_large: bool },
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
	fn fromTypeNullable(
		comptime T: type,
		comptime is_large: bool,
		comptime is_utf8: bool,
		comptime is_nullable: bool
	) Self {
		// https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
		return switch (@typeInfo(T)) {
			.Void, .Null => .null,
			.Bool => Self { .bool = .{ .is_nullable = is_nullable } },
			.Int => |info| switch (info.bits) {
				64 => switch (info.signedness) {
					.signed => Self { .i64 = .{ .is_nullable = is_nullable } },
					.unsigned => Self { .u64 = .{ .is_nullable = is_nullable } },
				},
				32 => switch (info.signedness) {
					.signed => Self { .i32 = .{ .is_nullable = is_nullable } },
					.unsigned => Self { .u32 = .{ .is_nullable = is_nullable } },
				},
				16 => switch (info.signedness) {
					.signed => Self { .i16 = .{ .is_nullable = is_nullable } },
					.unsigned => Self { .u16 = .{ .is_nullable = is_nullable } },
				},
				8 => switch (info.signedness) {
					.signed => Self { .i8 = .{ .is_nullable = is_nullable } },
					.unsigned => Self { .u8 = .{ .is_nullable = is_nullable } },
				},
				else => |w| @compileError(std.fmt.comptimePrint("unsupported int width {}", .{w})),
			},
			.Float => |info| switch (info.bits) {
				64 => Self { .f64 = .{ .is_nullable = is_nullable } },
				32 => Self { .f32 = .{ .is_nullable = is_nullable } },
				16 => Self { .f16 = .{ .is_nullable = is_nullable } },
				else => |w| @compileError(std.fmt.comptimePrint("unsupported float width {}", .{w})),
			},
			.Pointer => |p| switch (p.size) {
				.Slice => switch (p.child) {
					u8, ?u8 => Self { .binary = .{ .is_large = is_large, .is_utf8 = is_utf8 } },
					else => Self { .list = .{ .is_nullable = is_nullable, .is_large = is_large } },
				},
				else => @compileError("unsupported abi type " ++ @typeName(T))
			},
			.Optional => |o| fromTypeNullable(o.child, is_large, is_utf8, true),
			// .Array => |a| {
			// 	const byte_size = a.len * @sizeOf(a.child);
			// 	return std.fmt.comptimePrint("w:{d}", .{byte_size});
			// },
			else => @compileError("unsupported abi type " ++ @typeName(T)),
		};
	}

	pub fn fromType(comptime T: type, comptime is_large: bool, comptime is_utf8: bool) Self {
		return fromTypeNullable(T, is_large, is_utf8, false);
	}

	test "tag types" {
		try std.testing.expectEqual(Tag.u8, Tag.fromType(u8, false, false));
		try std.testing.expectEqual(Tag.i32, Tag.fromType(?i32, false, false));
		try std.testing.expectEqual(Tag.binary, Tag.fromType([]u8, false, false));
		try std.testing.expectEqual(Tag.binary, Tag.fromType([]?u8, false, false));
		try std.testing.expectEqual(Tag.binary, Tag.fromType([]u8, true, false));
		try std.testing.expectEqual(Tag.binary, Tag.fromType([]u8, false, true));
		try std.testing.expectEqual(Tag.binary, Tag.fromType([]u8, true, true));
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
			.u8, .binary => u8,
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
			.binary => .VariableBinary,
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
			.binary => |b| switch (b.is_unicode) {
				true => if (b.is_large) "U" else "u",
				false => if (b.is_large) "Z" else "z",
			},
			.list => "+l"
		};
	}
};

pub const MaskInt = std.bit_set.DynamicBitSet.MaskInt;
pub const Array = struct {
	tag: Tag, // 1
	allocator: std.mem.Allocator, // 2
	null_count: i64, // 1
	// TODO: align(64)
	validity: []MaskInt, // 2
	offsets: []align(64) u8, // 2
	values: []align(64) u8, // 2
	children: []Array, // 2

	pub fn deinit(self: @This()) void {
		for (self.children) |c| {
			c.deinit();
		}

		// See bit_set.zig#deinit
		if (self.validity.len > 0) {
			const old_allocation = (self.validity.ptr - 1)[0..(self.validity.ptr - 1)[0]];
			self.allocator.free(old_allocation);
		}

		self.allocator.free(self.offsets);
		self.allocator.free(self.values);
		self.allocator.free(self.children);
	}

	pub fn values_as(self: @This(), comptime T: type) []T {
		return std.mem.bytesAsSlice(T, self.values);
	}
};

test "array size" {
	std.debug.print("{d}\n", .{@sizeOf(Array) / 8});
}
