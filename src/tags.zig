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
	is_nullable: bool,
	is_large: bool = false,
};

pub const FixedListOptions = struct {
	is_nullable: bool,
	fixed_len: i16,
	is_large: bool = false,
};

pub const UnionOptions = struct {
	is_nullable: bool, // Just used for zig type safety.
	is_dense: bool = true,
};

pub const DictIndex = enum {
	// i64, // Currently zig hashmaps use u32s for capacity.
	i32,
	i16,
	i8,
};

pub const DictOptions = struct {
	index: DictIndex,
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
	list_fixed: FixedListOptions,
	struct_: PrimitiveOptions,
	union_: UnionOptions,
	dictionary: DictOptions,
	// Decimal128(u8, i8),
	// Decimal256(u8, i8),
	// Map(FieldRef, bool),
	// RunEndEncoded(FieldRef, FieldRef),

	const Self = @This();
	pub fn fromPrimitive(comptime T: type, comptime opts: BinaryOptions) Self {
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
			else => @compileError("unsupported abi type " ++ @typeName(T)),
		};
	}

	test "tag types" {
		try std.testing.expectEqual(Tag.u8, Tag.fromPrimitive(u8, .{}));
		try std.testing.expectEqual(Tag.i32, Tag.fromPrimitive(?i32, .{}));
		try std.testing.expectEqual(Tag.binary, Tag.fromPrimitive([]u8, .{}));
		try std.testing.expectEqual(Tag.binary, Tag.fromPrimitive([]?u8, .{}));
	}

	pub fn Primitive(comptime self: Self) type {
		return switch (self) {
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
			else => @compileError(@tagName(self) ++ " is not a primitive")
		};
	}

	pub fn abiLayout(self: Self) abi.Array.Layout {
		// https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
		// https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
		return switch (self) {
			.bool, .i64, .i32, .i16, .i8, .u64, .u32, .u16, .u8, .f64, .f32, .f16, => .Primitive,
			.binary => .VariableBinary,
			.list => .List,
			.list_fixed => .FixedList,
			.struct_ => .Struct,
			.union_ => |u| if (u.is_dense) .DenseUnion else .SparseUnion,
			.null => .Null,
			.dictionary => .Dictionary,
		};
	}

	pub fn abiFormat(self: Self, allocator: std.mem.Allocator, n_children: usize) ![*:0]const u8 {
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
			.binary => |b| switch (b.is_utf8) {
				true => if (b.is_large) "U" else "u",
				false => if (b.is_large) "Z" else "z",
			},
			.list => "+l",
			.list_fixed => |l| @ptrCast([*:0]const u8, (try std.fmt.allocPrint(allocator, "+w:{d}\x00", .{ l.fixed_len })).ptr),
			.struct_ => "+s",
			.union_ => |u| brk: {
				const prefix = if (u.is_dense) "+ud:" else "+us:";
				var res = std.ArrayList(u8).init(allocator);
				try res.writer().print("{s}", .{ prefix });
				for (0..n_children) |i| {
					if (i != n_children - 1) {
						try res.writer().print("{d},", .{ i });
					} else {
						try res.writer().print("{d}", .{ i });
					}
				}
				try res.append(0);
				break :brk @ptrCast([*:0]const u8, res.items.ptr);
			},
			.dictionary => |d| switch (d.index) {
				.i32 => "i",
				.i16 => "s",
				.i8 => "c",
			}
		};
	}

	pub fn isAbiFormatOnHeap(self: Self) bool {
		return switch (self) {
			.list_fixed, .union_ => true,
			else => false
		};
	}

	pub fn isNullable(self: Self) bool {
		return switch (self) {
			.bool, .i64, .i32, .i16, .i8, .u64, .u32, .u16, .u8, .f64, .f32, .f16, .struct_ => |opts| opts.is_nullable,
			.list_fixed => |opts| opts.is_nullable,
			.list => |opts| opts.is_nullable,
			.union_ => |opts| opts.is_nullable,
			else => false
		};
	}
};

