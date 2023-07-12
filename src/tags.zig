const std = @import("std");
const Layout = @import("ffi/abi.zig").Array.Layout;
const flat = @import("array/flat.zig");

const log = std.log.scoped(.arrow);

pub const AbiError = error{
    InvalidFormatString,
};

pub const NullableOptions = struct {
    nullable: bool,
};

pub const IntOptions = struct {
    const Self = @This();
    pub const BitWidth = enum {
        _8,
        _16,
        _32,
        _64,
    };
    nullable: bool,
    signed: bool,
    bit_width: BitWidth,
};

pub const FloatOptions = struct {
    pub const BitWidth = enum {
        _16,
        _32,
        _64,
    };
    nullable: bool,
    bit_width: BitWidth,
};

pub const BinaryOptions = struct {
    nullable: bool,
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
    // large: bool = false,
};

pub const UnionOptions = struct {
    nullable: bool, // Just used for zig type safety.
    dense: bool = true,
};

pub const DictOptions = struct {
    pub const Index = enum {
        i8,
        i16,
        i32,

        const Self = @This();

        // i64, // Currently zig hashmaps use u32s for capacity.
        pub fn Type(comptime self: Self) type {
            return switch (self) {
                .i32 => i32,
                .i16 => i16,
                .i8 => i8,
            };
        }
    };

    index: Index,
};

pub const DateOptions = struct {
    nullable: bool,
    unit: enum {
        day, // i32
        millisecond, // i64
    },
};

const TimeUnit = enum {
    second, // i32
    millisecond, // i32
    microsecond, // i64
    nanosecond, // i64

    pub fn bitWidth(self: @This()) i32 {
        return switch (self) {
            .second, .millisecond => 32,
            .microsecond, .nanosecond => 64,
        };
    }
};

pub const TimeOptions = struct {
    nullable: bool,
    unit: TimeUnit,
};

pub const TimestampOptions = struct {
    nullable: bool,
    unit: TimeUnit,
    timezone: [:0]const u8,
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
    Map: NullableOptions,
    // RunEndEncoded(FieldRef, FieldRef),

    const Self = @This();
    pub fn fromPrimitive(comptime T: type, comptime opts: BinaryOptions) Self {
        const is_nullable = @typeInfo(T) == .Optional;
        const ChildType = if (is_nullable) @typeInfo(T).Optional.child else T;
        // https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
        return switch (@typeInfo(ChildType)) {
            .Void, .Null => .null,
            .Bool => .{ .Bool = .{ .nullable = is_nullable } },
            .Int => |info| .{ .Int = .{
                .nullable = is_nullable,
                .signed = switch (info.signedness) {
                    .signed => true,
                    .unsigned => false,
                },
                .bit_width = switch (info.bits) {
                    8 => ._8,
                    16 => ._16,
                    32 => ._32,
                    64 => ._64,
                    else => |w| @compileError(std.fmt.comptimePrint("unsupported int width {}", .{w})),
                },
            } },
            .Float => |info| .{ .Float = .{
                .nullable = is_nullable,
                .bit_width = switch (info.bits) {
                    16 => ._16,
                    32 => ._32,
                    64 => ._64,
                    else => |w| @compileError(std.fmt.comptimePrint("unsupported float width {}", .{w})),
                },
            } },
            .Pointer => |p| switch (p.size) {
                .Slice => switch (p.child) {
                    u8, ?u8 => .{ .Binary = opts },
                    else => @compileError("unsupported slice type " ++ @typeName(T)),
                },
                else => @compileError("unsupported abi type " ++ @typeName(T)),
            },
            .Array => |a| switch (a.child) {
                u8, ?u8 => .{ .FixedBinary = .{ .nullable = is_nullable, .fixed_len = a.len } },
                else => @compileError("unsupported array type " ++ @typeName(T)),
            },
            else => @compileError("unsupported abi type " ++ @typeName(T)),
        };
    }

    test "tag types" {
        try std.testing.expectEqual(Tag.Int, Tag.fromPrimitive(u8, .{ .nullable = false }));
        try std.testing.expectEqual(Tag.Int, Tag.fromPrimitive(?i32, .{ .nullable = true }));
        try std.testing.expectEqual(Tag.Binary, Tag.fromPrimitive([]u8, .{ .nullable = false }));
        try std.testing.expectEqual(Tag.Binary, Tag.fromPrimitive([]?u8, .{ .nullable = true }));
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
            .Binary, .FixedBinary => u8,
            .Float => |f| switch (f.bit_width) {
                ._16 => f16,
                ._32 => f32,
                ._64 => f64,
            },
            else => @compileError(@tagName(self) ++ " is not a primitive"),
        };
    }

    pub fn abiLayout(self: Self) Layout {
        // https://github.com/ziglang/zig/blob/94e30a756edc4c2182168dabd97d481b8aec0ff2/lib/std/builtin.zig#L228
        // https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
        return switch (self) {
            .Bool,
            .Int,
            .Float,
            .Date,
            .Time,
            .Timestamp,
            .Duration,
            .Interval,
            .FixedBinary,
            => .Primitive,
            .Binary => .VariableBinary,
            .List => .List,
            .FixedList => .FixedList,
            .Struct => .Struct,
            .Union => |u| if (u.dense) .DenseUnion else .SparseUnion,
            .Null => .Null,
            .Dictionary => .Dictionary,
            .Map => .Map,
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
                try res.writer().print("{s}", .{prefix});
                try res.writer().print("{s}", .{ts.timezone});
                try res.append(0);
                return @ptrCast(res.items.ptr);
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
            .FixedBinary => |b| try std.fmt.allocPrintZ(allocator, "w:{d}", .{b.fixed_len}),
            .List => |l| if (l.large) "+L" else "+l",
            .FixedList => |l| try std.fmt.allocPrintZ(allocator, "+w:{d}", .{l.fixed_len}),
            .Struct => "+s",
            .Union => |u| {
                const prefix = if (u.dense) "+ud:" else "+us:";
                var res = std.ArrayList(u8).init(allocator);
                try res.writer().print("{s}", .{prefix});
                for (0..n_children) |i| {
                    if (i != n_children - 1) {
                        try res.writer().print("{d},", .{i});
                    } else {
                        try res.writer().print("{d}", .{i});
                    }
                }
                try res.append(0);
                return @ptrCast(res.items.ptr);
            },
            .Dictionary => |d| switch (d.index) {
                .i8 => "c",
                .i16 => "s",
                .i32 => "i",
            },
            .Map => "+m",
        };
    }

    pub fn isAbiFormatOnHeap(self: Self) bool {
        return switch (self) {
            .Timestamp, .FixedBinary, .FixedList, .Union => true,
            else => false,
        };
    }

    pub fn size(self: Self) usize {
        return switch (self) {
            .Bool => 1,
            .Int => |i| switch (i.bit_width) {
                ._8 => 1,
                ._16 => 2,
                ._32 => 4,
                ._64 => 8,
            },
            .Float => |f| switch (f.bit_width) {
                ._16 => 2,
                ._32 => 4,
                ._64 => 8,
            },
            .Date => |d| switch (d.unit) {
                .day => 4,
                .millisecond => 8,
            },
            .Time => |t| switch (t.unit) {
                .second, .millisecond => 4,
                .microsecond, .nanosecond => 8,
            },
            .Timestamp => 8,
            .Duration => 8,
            .Interval => |i| switch (i.unit) {
                .year_month => 4,
                .day_time => 8,
                .month_day_nanosecond => 16,
            },
            .Binary => 1,
            .FixedBinary => |b| @intCast(b.fixed_len),
            .Dictionary => |d| switch (d.index) {
                .i8 => 1,
                .i16 => 2,
                .i32 => 4,
            },
            else => 0,
        };
    }

    pub fn offsetSize(self: Self) usize {
        return switch (self) {
            .Binary => |b| if (b.large) 8 else 4,
            .List => |l| if (l.large) 8 else 4,
            .FixedList => 4,
            .Union => |u| if (u.dense) 4 else 0,
            else => 0,
        };
    }

    fn fromAbiFormat2(format: [:0]const u8, is_nullable: bool, is_dictionary: bool) !Self {
        var stream = std.io.fixedBufferStream(format);
        var reader = stream.reader();

        switch (try reader.readByte()) {
            'n' => return .Null,
            'b' => return .{ .Bool = .{ .nullable = is_nullable } },
            'c' => return if (is_dictionary)
                .{ .Dictionary = .{ .index = .i8 } }
            else
                .{ .Int = .{ .nullable = is_nullable, .signed = true, .bit_width = ._8 } },
            'C' => return .{ .Int = .{ .nullable = is_nullable, .signed = false, .bit_width = ._8 } },
            's' => return if (is_dictionary)
                .{ .Dictionary = .{ .index = .i16 } }
            else
                .{ .Int = .{ .nullable = is_nullable, .signed = true, .bit_width = ._16 } },
            'S' => return .{ .Int = .{ .nullable = is_nullable, .signed = false, .bit_width = ._16 } },
            'i' => return if (is_dictionary)
                .{ .Dictionary = .{ .index = .i32 } }
            else
                .{ .Int = .{ .nullable = is_nullable, .signed = true, .bit_width = ._32 } },
            'I' => return .{ .Int = .{ .nullable = is_nullable, .signed = false, .bit_width = ._32 } },
            'l' => return .{ .Int = .{ .nullable = is_nullable, .signed = true, .bit_width = ._64 } },
            'L' => return .{ .Int = .{ .nullable = is_nullable, .signed = false, .bit_width = ._64 } },
            'e' => return .{ .Float = .{ .nullable = is_nullable, .bit_width = ._16 } },
            'f' => return .{ .Float = .{ .nullable = is_nullable, .bit_width = ._32 } },
            'g' => return .{ .Float = .{ .nullable = is_nullable, .bit_width = ._64 } },
            'u' => return .{ .Binary = .{ .nullable = is_nullable, .large = false, .utf8 = true } },
            'U' => return .{ .Binary = .{ .nullable = is_nullable, .large = true, .utf8 = true } },
            'z' => return .{ .Binary = .{ .nullable = is_nullable, .large = false, .utf8 = false } },
            'Z' => return .{ .Binary = .{ .nullable = is_nullable, .large = true, .utf8 = false } },
            'd' => {
                var td: [2]u8 = undefined;
                _ = try reader.read(&td);
                if (td[0] == 't' and td[1] == 'd') {
                    switch (try reader.readByte()) {
                        'D' => return .{ .Date = .{ .nullable = is_nullable, .unit = .day } },
                        'm' => return .{ .Date = .{ .nullable = is_nullable, .unit = .millisecond } },
                        else => {},
                    }
                }
            },
            't' => {
                switch (try reader.readByte()) {
                    't' => {
                        switch (try reader.readByte()) {
                            's' => return .{ .Time = .{ .nullable = is_nullable, .unit = .second } },
                            'm' => return .{ .Time = .{ .nullable = is_nullable, .unit = .millisecond } },
                            'u' => return .{ .Time = .{ .nullable = is_nullable, .unit = .microsecond } },
                            'n' => return .{ .Time = .{ .nullable = is_nullable, .unit = .nanosecond } },
                            'D' => {
                                switch (try reader.readByte()) {
                                    's' => return .{ .Duration = .{ .nullable = is_nullable, .unit = .second } },
                                    'm' => return .{ .Duration = .{ .nullable = is_nullable, .unit = .millisecond } },
                                    'u' => return .{ .Duration = .{ .nullable = is_nullable, .unit = .microsecond } },
                                    'n' => return .{ .Duration = .{ .nullable = is_nullable, .unit = .nanosecond } },
                                    else => {},
                                }
                            },
                            'i' => switch (try reader.readByte()) {
                                'M' => return .{ .Interval = .{ .nullable = is_nullable, .unit = .year_month } },
                                'D' => return .{ .Interval = .{ .nullable = is_nullable, .unit = .day_time } },
                                'n' => return .{ .Interval = .{ .nullable = is_nullable, .unit = .month_day_nanosecond } },
                                else => {},
                            },
                            else => {},
                        }
                    },
                    's' => {
                        switch (try reader.readByte()) {
                            's' => return .{ .Timestamp = .{
                                .nullable = is_nullable,
                                .unit = .second,
                                .timezone = format[4..],
                            } },
                            'm' => return .{ .Timestamp = .{
                                .nullable = is_nullable,
                                .unit = .millisecond,
                                .timezone = format[4..],
                            } },
                            'u' => return .{ .Timestamp = .{
                                .nullable = is_nullable,
                                .unit = .microsecond,
                                .timezone = format[4..],
                            } },
                            'n' => return .{ .Timestamp = .{
                                .nullable = is_nullable,
                                .unit = .nanosecond,
                                .timezone = format[4..],
                            } },
                            else => {},
                        }
                    },
                    else => {},
                }
            },
            'w' => {
                const fixed_len = try std.fmt.parseInt(i32, format[2..], 10);
                return .{ .FixedBinary = .{ .nullable = is_nullable, .fixed_len = fixed_len } };
            },
            '+' => {
                switch (try reader.readByte()) {
                    'l' => return .{ .List = .{ .nullable = is_nullable, .large = false } },
                    'L' => return .{ .List = .{ .nullable = is_nullable, .large = true } },
                    'w' => {
                        const fixed_len = try std.fmt.parseInt(i32, format[3..], 10);
                        return .{ .FixedList = .{ .nullable = is_nullable, .fixed_len = fixed_len } };
                    },
                    'u' => {
                        switch (try reader.readByte()) {
                            'd' => return .{ .Union = .{ .nullable = is_nullable, .dense = true } },
                            's' => return .{ .Union = .{ .nullable = is_nullable, .dense = false } },
                            else => {},
                        }
                    },
                    'm' => return .{ .Map = .{ .nullable = is_nullable } },
                    's' => return .{ .Struct = .{ .nullable = is_nullable } },
                    else => {},
                }
            },
            else => {},
        }

        return AbiError.InvalidFormatString;
    }

    pub fn fromAbiFormat(format: [:0]const u8, is_nullable: bool, is_dictionary: bool) !Self {
        const res = fromAbiFormat2(format, is_nullable, is_dictionary) catch |err| {
            log.err("could not parse abi format string '{s}': {any}", .{ format, err });
            return err;
        };

        return res;
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
            .Binary => |opts| opts.nullable,
            .FixedBinary => |opts| opts.nullable,
            .List => |opts| opts.nullable,
            .FixedList => |opts| opts.nullable,
            .Struct => |opts| opts.nullable,
            .Union => |opts| opts.nullable,
            .Dictionary => true,
            .Map => |opts| opts.nullable,
        };
    }
};
