const std = @import("std");
const abi = @import("./abi.zig");
const flat = @import("./array/flat.zig");

pub const PrimitiveOptions = struct {
	is_nullable: bool
};

pub const BinaryOptions = struct {
	is_large: bool = false,
	is_utf8: bool = false
};

pub const ListOptions = struct {
 is_large: bool = false,
 is_nullable: bool
};

pub const Tag = union(enum) {
	null,
	bool: PrimitiveOptions,
	i64: PrimitiveOptions,
	i32: PrimitiveOptions,
	i16: PrimitiveOptions,
	i8: PrimitiveOptions,
	u64: PrimitiveOptions,
	u32: PrimitiveOptions,
	u16: PrimitiveOptions,
	u8: PrimitiveOptions,
	f64: PrimitiveOptions,
	f32: PrimitiveOptions,
	f16: PrimitiveOptions,
	// Timestamp(TimeUnit, Option<Arc<str>>),
	// date64,
	// date32,
	// Time32(TimeUnit),
	// Time64(TimeUnit),
	// Duration(TimeUnit),
	// Interval(IntervalUnit),
	binary: BinaryOptions,
	// FixedSizeBinary(i32),
	list: ListOptions,
	list_fixed: PrimitiveOptions,
	struct_: PrimitiveOptions,
	// Struct(Fields),
	// Union(UnionFields, UnionMode),
	// Dictionary(Box<DataType>, Box<DataType>),
	// Decimal128(u8, i8),
	// Decimal256(u8, i8),
	// Map(FieldRef, bool),
	// RunEndEncoded(FieldRef, FieldRef),

	const Self = @This();
	pub fn fromPrimitiveType(comptime T: type, comptime opts: BinaryOptions) Self {
		const is_nullable = @typeInfo(T) == .Optional;
		const ChildType = if (is_nullable) @typeInfo(T).Optional.child else T;
		const primitive_opts = PrimitiveOptions { .is_nullable = is_nullable };
		// https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
		return switch (@typeInfo(ChildType)) {
			.Void, .Null => .null,
			.Bool => Self { .bool = primitive_opts },
			.Int => |info| switch (info.bits) {
				64 => switch (info.signedness) {
					.signed => Self { .i64 = primitive_opts },
					.unsigned => Self { .u64 = primitive_opts },
				},
				32 => switch (info.signedness) {
					.signed => Self { .i32 = primitive_opts },
					.unsigned => Self { .u32 = primitive_opts },
				},
				16 => switch (info.signedness) {
					.signed => Self { .i16 = primitive_opts },
					.unsigned => Self { .u16 = primitive_opts },
				},
				8 => switch (info.signedness) {
					.signed => Self { .i8 = primitive_opts },
					.unsigned => Self { .u8 = primitive_opts },
				},
				else => |w| @compileError(std.fmt.comptimePrint("unsupported int width {}", .{w})),
			},
			.Float => |info| switch (info.bits) {
				64 => Self { .f64 = primitive_opts },
				32 => Self { .f32 = primitive_opts },
				16 => Self { .f16 = primitive_opts },
				else => |w| @compileError(std.fmt.comptimePrint("unsupported float width {}", .{w})),
			},
			.Pointer => |p| switch (p.size) {
				.Slice => switch (p.child) {
					u8, ?u8 => Self { .binary = opts },
					else => @compileError("unsupported slice type " ++ @typeName(T))
				},
				else => @compileError("unsupported abi type " ++ @typeName(T))
			},
			// .Array => |a| {
			// 	const byte_size = a.len * @sizeOf(a.child);
			// 	return std.fmt.comptimePrint("w:{d}", .{byte_size});
			// },
			else => @compileError("unsupported abi type " ++ @typeName(T)),
		};
	}

	test "tag types" {
		try std.testing.expectEqual(Tag.u8, Tag.fromPrimitiveType(u8, .{}));
		try std.testing.expectEqual(Tag.i32, Tag.fromPrimitiveType(?i32, .{}));
		try std.testing.expectEqual(Tag.binary, Tag.fromPrimitiveType([]u8, .{}));
		try std.testing.expectEqual(Tag.binary, Tag.fromPrimitiveType([]?u8, .{}));
	}

	pub fn ValueType(comptime self: Self) type {
		return switch (self) {
			.null, .list, .list_fixed, .struct_ => void,
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
			.list_fixed => .FixedList,
			.struct_ => .Struct,
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
			.list => "+l",
			.list_fixed => "+w",
			.struct_ => "+s",
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

		if (self.offsets.len > 0) {
			self.allocator.free(self.offsets);
		}
		if (self.values.len > 0) {
			self.allocator.free(self.values);
		}
		if (self.children.len > 0) {
			self.allocator.free(self.children);
		}
	}

	pub fn values_as(self: @This(), comptime T: type) []T {
		return std.mem.bytesAsSlice(T, self.values);
	}
};

fn alloc(_: *anyopaque, _: usize, _: u8, _: usize) ?[*]u8 { return null; }
fn resize(_: *anyopaque, _: []u8, _: u8, _: usize, _: usize) bool { return false; }
fn free(_: *anyopaque, _: []u8, _: u8, _: usize) void {}

pub const null_array = Array {
	.tag = .null,
	.allocator = std.mem.Allocator {
		.ptr = undefined,
		.vtable = &std.mem.Allocator.VTable {
			.alloc = alloc,
			.resize = resize,
			.free = free,
		}
	},
	.null_count = 0,
	.validity = &[_]MaskInt{},
	.offsets = &[_]u8{},
	.values = &[_]u8{},
	.children = &[_]Array{},
};

test "null array" {
	const n = null_array;
	defer n.deinit();
	try std.testing.expectEqual(@as(i64, 0), n.null_count);
}
