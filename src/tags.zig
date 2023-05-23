const std = @import("std");
const abi = @import("./abi.zig");
const flat = @import("./array/flat.zig");

pub const NullableOptions = struct {
	nullable: bool,
};

pub const IntOptions = struct {
	nullable: bool,
	signed: bool,
	bit_width: enum {
		_8,
		_16,
		_32,
		_64,
	},
};

pub const FloatOptions = struct {
	nullable: bool,
	bit_width: enum {
		_16,
		_32,
		_64,
	},
};

pub const BinaryOptions = struct {
	large: bool = false,
	utf8: bool = false,
};

pub const FixedBinaryOptions = struct {
	nullable: bool,
	fixed_len: i32,
};

pub const ListOptions = struct {
	nullable: bool,
	large: bool = false,
};

pub const FixedListOptions = struct {
	nullable: bool,
	fixed_len: i32,
	large: bool = false,
};

pub const UnionOptions = struct {
	nullable: bool, // Just used for zig type safety.
	dense: bool = true,
};

pub const DictOptions = struct {
	index: enum {
		i8,
		i16,
		i32,
		// i64, // Currently zig hashmaps use u32s for capacity.
	},
};

pub const DateOptions = struct {
	nullable: bool,
	unit: enum {
		day,
		millisecond
	},
};

const TimeUnit = enum {
	second, // i32
	millisecond, // i32
	microsecond, // i64
	nanosecond, // i64
};

pub const TimeOptions = struct {
	nullable: bool,
	unit: TimeUnit,
};

pub const TimestampOptions = struct {
	nullable: bool,
	unit: TimeUnit,
	timezone: []const u8,
};

pub const IntervalOptions = struct {
	nullable: bool,
	unit: enum {
		year_month,
		day_time,
		month_day_nanosecond,
	},
};

pub const Tag = union(enum) {
	Null,
	Bool: NullableOptions,
	Int: IntOptions,
	Float: FloatOptions,
	Date: DateOptions,
	Time: TimeOptions,
	Timestamp: TimestampOptions,
	Duration: TimeOptions,
	Interval: IntervalOptions,
	Binary: BinaryOptions,
	FixedBinary: FixedBinaryOptions,
	List: ListOptions,
	FixedList: FixedListOptions,
	Struct: NullableOptions,
	Union: UnionOptions,
	Dictionary: DictOptions,
	// Decimal128(u8, i8),
	// Decimal256(u8, i8),
	// Map(FieldRef, bool),
	// RunEndEncoded(FieldRef, FieldRef),

	const Self = @This();
	pub fn fromPrimitive(comptime T: type, comptime opts: BinaryOptions) Self {
		const is_nullable = @typeInfo(T) == .Optional;
		const ChildType = if (is_nullable) @typeInfo(T).Optional.child else T;
		// https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
		return switch (@typeInfo(ChildType)) {
			.Void, .Null => .null,
			.Bool => .{ .Bool = .{ .nullable = is_nullable } },
			.Int => |info| .{
				.Int = .{
					.nullable = is_nullable,
					.signed = switch (info.signedness) {
						.signed => true,
						.unsigned => false
					},
					.bit_width = switch (info.bits) {
						8 => ._8,
						16 => ._16,
						32 => ._32,
						64 => ._64,
						else => |w| @compileError(std.fmt.comptimePrint("unsupported int width {}", .{w})),
					}
				}
			},
			.Float => |info| .{
				.Float = .{
					.nullable = is_nullable,
					.bit_width = switch (info.bits) {
						16 => ._16,
						32 => ._32,
						64 => ._64,
						else => |w| @compileError(std.fmt.comptimePrint("unsupported float width {}", .{w})),
					}
				}
			},
			.Pointer => |p| switch (p.size) {
				.Slice => switch (p.child) {
					u8, ?u8 => .{ .Binary = opts },
					else => @compileError("unsupported slice type " ++ @typeName(T))
				},
				else => @compileError("unsupported abi type " ++ @typeName(T))
			},
			.Struct => .{ .Struct = .{ .nullable = is_nullable } },
			.Union => .{ .Union = .{ .nullable = is_nullable } },
			else => @compileError("unsupported abi type " ++ @typeName(T)),
		};
	}

	test "tag types" {
		try std.testing.expectEqual(Tag.Int, Tag.fromPrimitive(u8, .{}));
		try std.testing.expectEqual(Tag.Int, Tag.fromPrimitive(?i32, .{}));
		try std.testing.expectEqual(Tag.Binary, Tag.fromPrimitive([]u8, .{}));
		try std.testing.expectEqual(Tag.Binary, Tag.fromPrimitive([]?u8, .{}));
	}

	pub fn Primitive(comptime self: Self) type {
		return switch (self) {
			.Null => void,
			.Bool => bool,
			.Int => |i| switch (i.bit_width) {
				._8 => if (i.signed) i8 else u8,
				._16 => if (i.signed) i16 else u16,
				._32 => if (i.signed) i32 else u32,
				._64 => if (i.signed) i64 else u64,
			},
			.Binary => u8,
			.Float => |f| switch (f.bit_width) {
				._16 => f16,
				._32 => f32,
				._64 => f64,
			},
			else => @compileError(@tagName(self) ++ " is not a primitive")
		};
	}

	pub fn abiLayout(self: Self) abi.Array.Layout {
		// https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
		// https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
		return switch (self) {
			.Bool, .Int, .Float, .Date, .Time, .Timestamp, .Duration, .Interval, .FixedBinary, => .Primitive,
			.Binary => .VariableBinary,
			.List => .List,
			.FixedList => .FixedList,
			.Struct => .Struct,
			.Union => |u| if (u.dense) .DenseUnion else .SparseUnion,
			.Null => .Null,
			.Dictionary => .Dictionary,
		};
	}

	pub fn abiFormat(self: Self, allocator: std.mem.Allocator, n_children: usize) ![*:0]const u8 {
		return switch (self) {
			.Null => "n",
			.Bool => "b",
			.Int => |i| switch (i.bit_width) {
				._8 => if (i.signed) "c" else "C",
				._16 => if (i.signed) "s" else "S",
				._32 => if (i.signed) "i" else "I",
				._64 => if (i.signed) "l" else "L",
			},
			.Float => |f| switch (f.bit_width) {
				._16 => "e",
				._32 => "f",
				._64 => "g",
			},
			.Date => |d| switch (d.unit) {
				.day => "tdD",
				.millisecond => "tdm",
			},
			.Time => |t| switch (t.unit) {
				.second => "tts",
				.millisecond => "ttm",
				.microsecond => "ttu",
				.nanosecond => "ttn",
			},
			.Timestamp => |ts| {
				const prefix = switch (ts.unit) {
					.second => "tss:",
					.millisecond => "tsm:",
					.microsecond => "tsu:",
					.nanosecond => "tsn:",
				};
				var res = std.ArrayList(u8).init(allocator);
				try res.writer().print("{s}", .{ prefix });
				try res.writer().print("{s}", .{ ts.timezone });
				try res.append(0);
				return @ptrCast([*:0]const u8, res.items.ptr);
			},
			.Duration => |d| switch (d.unit) {
				.second => "tDs",
				.millisecond => "tDm",
				.microsecond => "tDu",
				.nanosecond => "tDn",
			},
			.Interval => |i| switch (i.unit) {
				.year_month => "tiM",
				.day_time => "tiD",
				.month_day_nanosecond => "tin",
			},
			.Binary => |b| switch (b.utf8) {
				true => if (b.large) "U" else "u",
				false => if (b.large) "Z" else "z",
			},
			.FixedBinary => |b| {
				const res = try std.fmt.allocPrint(allocator, "w:{d}\x00", .{ b.fixed_len });
				return @ptrCast([*:0]const u8, res.ptr);
			},
			.List => "+l",
			.FixedList => |l| {
				const res = try std.fmt.allocPrint(allocator, "+w:{d}\x00", .{ l.fixed_len });
				return @ptrCast([*:0]const u8, res.ptr);
			},
			.Struct => "+s",
			.Union => |u| {
				const prefix = if (u.dense) "+ud:" else "+us:";
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
				return @ptrCast([*:0]const u8, res.items.ptr);
			},
			.Dictionary => |d| switch (d.index) {
				.i8 => "c",
				.i16 => "s",
				.i32 => "i",
			}
		};
	}

	pub fn isAbiFormatOnHeap(self: Self) bool {
		return switch (self) {
			.Timestamp, .FixedBinary, .FixedList, .Union => true,
			else => false
		};
	}

	pub fn nullable(self: Self) bool {
		return switch (self) {
			.Null => false,
			.Bool => |opts| opts.nullable,
			.Int => |opts| opts.nullable,
			.Float => |opts| opts.nullable,
			.Date => |opts| opts.nullable,
			.Time => |opts| opts.nullable,
			.Timestamp => |opts| opts.nullable,
			.Duration => |opts| opts.nullable,
			.Interval => |opts| opts.nullable,
			.Binary => true,
			.FixedBinary => |opts| opts.nullable,
			.List => |opts| opts.nullable,
			.FixedList => |opts| opts.nullable,
			.Struct => |opts| opts.nullable,
			.Union => |opts| opts.nullable,
			.Dictionary => true,
		};
	}

	pub fn setNullable(self: *Self, is_nullable: bool) void {
		return switch (self.*) {
			.Null => std.debug.print("tried to set nullable on {any}\n", .{ self }),
			.Bool => |*opts| opts.nullable = is_nullable,
			.Int => |*opts| opts.nullable = is_nullable,
			.Float => |*opts| opts.nullable = is_nullable,
			.Date => |*opts| opts.nullable = is_nullable,
			.Time => |*opts| opts.nullable = is_nullable,
			.Timestamp => |*opts| opts.nullable = is_nullable,
			.Duration => |*opts| opts.nullable = is_nullable,
			.Interval => |*opts| opts.nullable = is_nullable,
			.Binary => std.debug.print("tried to set nullable on {any}\n", .{ self }),
			.FixedBinary => |*opts| opts.nullable = is_nullable,
			.List => |*opts| opts.nullable = is_nullable,
			.FixedList => |*opts| opts.nullable = is_nullable,
			.Struct => |*opts| opts.nullable = is_nullable,
			.Union => |*opts| opts.nullable = is_nullable,
			.Dictionary => std.debug.print("tried to set nullable on {any}\n", .{ self }),
		};
	}
};

