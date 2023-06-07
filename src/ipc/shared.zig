const std = @import("std");
const tags = @import("../tags.zig");
const Array = @import("../array/array.zig").Array;
const Schema = @import("./gen/Schema.fb.zig").Schema;
const Field = @import("./gen/Field.fb.zig").Field;
const DictionaryEncoding = @import("./gen/DictionaryEncoding.fb.zig").DictionaryEncoding;

const Int = @import("./gen/Int.fb.zig").Int;
const Float = @import("./gen/FloatingPoint.fb.zig").FloatingPoint;
const Date = @import("./gen/Date.fb.zig").Date;
const Time = @import("./gen/Time.fb.zig").Time;
const Timestamp = @import("./gen/Timestamp.fb.zig").Timestamp;
const Duration = @import("./gen/Duration.fb.zig").Duration;
const Interval = @import("./gen/Interval.fb.zig").Interval;
const Union = @import("./gen/Union.fb.zig").Union;
const FixedSizeBinary = @import("./gen/FixedSizeBinary.fb.zig").FixedSizeBinary;
const FixedSizeList = @import("./gen/FixedSizeList.fb.zig").FixedSizeList;

pub const buffer_alignment = Array.buffer_alignment;
pub const magic = "ARROW1";
pub const MessageLen = i32;
pub const continuation = @bitCast(MessageLen, @as(u32, 0xffffffff));
pub const log = std.log.scoped(.arrow);

pub const IpcError = error {
	InvalidFieldTag,
	InvalidDictionaryIndexType,
	InvalidBitWidth,
	InvalidLen,
};

fn nFields2(f: Field) usize {
	var res: usize = 0;
	for (0..f.ChildrenLen()) |i| res += 1 + nFields2(f.Children(i).?);
	return res;
}

pub fn nFields(schema: Schema) usize {
	var res: usize = 0;
	for (0..schema.FieldsLen()) |i| res += 1 + nFields2(schema.Fields(i).?);
	return res;
}

pub fn toDictTag(dict: DictionaryEncoding) !tags.Tag {
	return .{
		.Dictionary = .{
			.index = if (dict.IndexType()) |t| switch (t.BitWidth()) {
				8 => .i8,
				16 => .i16,
				32 => .i32,
				else => |w| {
					log.err("dictionary {d} has invalid index bit width {d}", .{ dict.Id(), w });
					return IpcError.InvalidDictionaryIndexType;
				}
			} else {
				log.err("dictionary {d} missing index type", .{ dict.Id() });
				return IpcError.InvalidDictionaryIndexType;
			}
		}
	};
}

pub fn toFieldTag(field: Field) !tags.Tag {
	const tb = field.Type_().?;
	return switch (field.TypeType()) {
		.Null => .Null,
		.Int => {
			var i = Int.init(tb.bytes, tb.pos);
			return .{
				.Int = .{
					.nullable = field.Nullable(),
					.signed = i.IsSigned(),
					.bit_width = switch (i.BitWidth()) {
						8 => ._8,
						16 => ._16,
						32 => ._32,
						64 => ._64,
						else => |w| {
							log.err("int field {s} invalid bit width {d}", .{ field.Name(), w });
							return IpcError.InvalidBitWidth;
						}
					}
				}
			};
		},
		.FloatingPoint => {
			var f = Float.init(tb.bytes, tb.pos);
			return .{
				.Float = .{
					.nullable = field.Nullable(),
					.bit_width = switch (f.Precision_()) {
						.HALF => ._16,
						.SINGLE => ._32,
						.DOUBLE => ._64,
					}
				}
			};
		},
		.Binary => .{ .Binary = .{ .large = false, .utf8 = false } },
		.LargeBinary => .{ .Binary = .{ .large = true, .utf8 = false } },
		.Utf8 => .{ .Binary = .{ .large = false, .utf8 = true } },
		.LargeUtf8 => .{ .Binary = .{ .large = true, .utf8 = true } },
		.Bool => .{ .Bool = .{ .nullable = field.Nullable() } },
		// .Decimal: ?*DecimalT,
		.Date => {
			var d = Date.init(tb.bytes, tb.pos);
			return .{
				.Date = .{
					.nullable = field.Nullable(),
					.unit = switch (d.Unit()) {
						.DAY => .day,
						.MILLISECOND => .millisecond,
					}
				}
			};
		},
		.Time => {
			var t = Time.init(tb.bytes, tb.pos);
			return .{
				.Time = .{
					.nullable = field.Nullable(),
					.unit = switch (t.Unit()) {
						.SECOND => .second,
						.MILLISECOND => .millisecond,
						.MICROSECOND => .microsecond,
						.NANOSECOND => .nanosecond,
					}
				}
			};
		},
		.Timestamp => {
			const ts = Timestamp.init(tb.bytes, tb.pos);
			return .{
				.Timestamp = .{
					.nullable = field.Nullable(),
					.unit = switch (ts.Unit()) {
						.SECOND => .second,
						.MILLISECOND => .millisecond,
						.MICROSECOND => .microsecond,
						.NANOSECOND => .nanosecond,
					},
					.timezone = ts.Timezone(),
				}
			};
		},
		.Duration => {
			const d = Duration.init(tb.bytes, tb.pos);
			return .{
				.Duration = .{
					.nullable = field.Nullable(),
					.unit = switch (d.Unit()) {
						.SECOND => .second,
						.MILLISECOND => .millisecond,
						.MICROSECOND => .microsecond,
						.NANOSECOND => .nanosecond,
					},
				}
			};
		},
		.Interval => {
			const i = Interval.init(tb.bytes, tb.pos);
			return .{
				.Interval = .{
					.nullable = field.Nullable(),
					.unit = switch (i.Unit()) {
						.YEAR_MONTH => .year_month,
						.DAY_TIME => .day_time,
						.MONTH_DAY_NANO => .month_day_nanosecond,
					},
				}
			};
		},
		.List => .{ .List = .{ .nullable = field.Nullable(), .large = false } },
		.LargeList => .{ .List = .{ .nullable = field.Nullable(), .large = true } },
		.Struct_ => .{ .Struct = .{ .nullable = field.Nullable() } },
		.Union => {
			const u = Union.init(tb.bytes, tb.pos);
			return .{
				.Union = .{
					.nullable = field.Nullable(),
					.dense = switch (u.Mode()) {
						.Dense => true,
						.Sparse => false,
					}
				}
			};
		},
		.FixedSizeBinary => {
			const b = FixedSizeBinary.init(tb.bytes, tb.pos);
			return .{
				.FixedBinary = .{
					.nullable = field.Nullable(),
					.fixed_len = b.ByteWidth(),
				}
			};
		},
		.FixedSizeList => {
			const f = FixedSizeList.init(tb.bytes, tb.pos);
			return .{
				.FixedList = .{
					.nullable = field.Nullable(),
					.fixed_len = f.ListSize(),
					.large = false,
				}
			};
		},
		.Map => .{ .Map = .{ .nullable = field.Nullable() } },
		// .RunEndEncoded: ?*RunEndEncodedT,
		else => |t| {
			log.warn("field {s} unknown type {any}", .{ field.Name(), t });
			return IpcError.InvalidFieldTag;
		}
	};
}

pub fn toTag(field: Field) !tags.Tag {
	if (field.Dictionary()) |d| return toDictTag(d);

	return toFieldTag(field);
}

fn nBuffers2(f: Field) !usize {
	var res: usize = 0;
	res += (try toTag(f)).abiLayout().nBuffers();
	for (0..f.ChildrenLen()) |i| {
		res += try nBuffers2(f.Children(i).?);
	}
	return res;
}

pub fn nBuffers(schema: Schema) !usize {
	var res: usize = 0;
	for (0..schema.FieldsLen()) |i| res += try nBuffers2(schema.Fields(i).?);
	return res;
}

