// Flat means no children. Includes Primitive and VariableBinary layouts.
const std = @import("std");
const tags = @import("../tags.zig");
const array = @import("./array.zig");

pub fn BuilderAdvanced(comptime T: type, comptime opts: tags.BinaryOptions) type {
	const tag = tags.Tag.fromPrimitiveType(T, opts);
	const layout = tag.abiLayout();
	if (layout != .Primitive and layout != .VariableBinary) {
		@compileError("unsupported flat type " ++ @typeName(T));
	}

	const NullCount = if (@typeInfo(T) == .Optional) i64 else void;
	// TODO: does this need to be 64 byte aligned?
	const ValidityList = if (@typeInfo(T) == .Optional) std.bit_set.DynamicBitSet else void;
	const ValueType = tag.ValueType();

	const OffsetType = if (opts.is_large) i64 else i32;
	const OffsetList = if (layout.hasOffsets()) std.ArrayListAligned(OffsetType, 64) else void;
	const ValueList = std.ArrayListAligned(ValueType, 64);

	return struct {
		const Self = @This();

		null_count: NullCount,
		validity: ValidityList,
		offsets: OffsetList,
		values: ValueList,

		pub fn Type() type {
			return T;
		}

		pub fn init(allocator: std.mem.Allocator) !Self {
			var res = Self {
				.null_count = if (NullCount != void) 0 else {},
				.validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
				.offsets = if (OffsetList != void) OffsetList.init(allocator) else {},
				.values = ValueList.init(allocator),
			};
			// dunno why this is in the spec:
			// > the offsets buffer contains length + 1 signed integers (either 32-bit or 64-bit,
			// > depending on the logical type), which encode the start position of each slot in the data
			// > buffer.
			if (OffsetList != void) {
				try res.offsets.append(0);
			}

			return res;
		}

		pub fn deinit(self: *Self) void {
			if (ValidityList != void) self.validity.deinit();
			if (OffsetList != void) self.offsets.deinit();
			self.values.deinit();
		}

		fn appendAny(self: *Self, value: anytype) std.mem.Allocator.Error!void {
			switch (@typeInfo(@TypeOf(value))) {
				.Bool, .Int, .Float, .ComptimeInt, .ComptimeFloat => try self.values.append(value),
				.Pointer => |p| switch (p.size) {
					.Slice => {
						std.debug.assert(layout == .VariableBinary);
						try self.values.appendSlice(value);
						try self.offsets.append(@intCast(OffsetType, self.values.items.len));
					},
					else => |t| @compileError("unsupported pointer type " ++ @tagName(t)),
				},
				.Null => {
					if (OffsetList != void) {
						try self.offsets.append(self.offsets.items[self.offsets.items.len - 1]);
					} else {
						// > Array slots which are null are not required to have a particular value; any
						// > "masked" memory can have any value and need not be zeroed, though implementations
						// > frequently choose to zero memory for null items.
						// PLEASE, for the sake of SIMD, 0 this
						try self.appendAny(0);
					}
				},
				.Optional => {
					const is_null = value == null;
					try self.validity.resize(self.validity.capacity() + 1, !is_null);
					if (is_null) {
						self.null_count += 1;
						try self.appendAny(null);
					} else {
						try self.appendAny(value.?);
					}
				},
				else => |t| @compileError("unsupported append type " ++ @tagName(t))
			}
		}

		pub fn append(self: *Self, value: T) std.mem.Allocator.Error!void {
			return self.appendAny(value);
		}

		pub fn finish(self: *Self) !array.Array {
			return .{
				.tag = tag,
				.allocator = self.values.allocator,
				.null_count = if (NullCount != void) self.null_count else 0,
				.validity = if (ValidityList != void) array.validity(&self.validity, self.null_count) else &[_]array.MaskInt{},
				.offsets = if (OffsetList != void) std.mem.sliceAsBytes(try self.offsets.toOwnedSlice()) else &[_]u8{},
				// TODO: implement @ptrCast between slices changing the length
				.values = std.mem.sliceAsBytes(try self.values.toOwnedSlice()),
				.children = &[_]array.Array{}
			};
		}
	};
}

pub fn Builder(comptime T: type) type {
	return BuilderAdvanced(T, .{ .is_large = false, .is_utf8 = false });
}

test "primitive init + deinit" {
	var b = try Builder(i32).init(std.testing.allocator);
	defer b.deinit();

	try b.append(32);
}

test "primitive optional" {
	var b = try Builder(?i32).init(std.testing.allocator);
	defer b.deinit();
	try b.append(1);
	try b.append(null);
	try b.append(2);
	try b.append(4);

	const masks = b.validity.unmanaged.masks;
	try std.testing.expectEqual(@as(array.MaskInt, 0b1101), masks[0]);
}

test "primitive finish" {
	const T = i32;
	var b = try Builder(?T).init(std.testing.allocator);
	try b.append(1);
	try b.append(null);
	try b.append(2);
	try b.append(4);

	const a = try b.finish();
	defer a.deinit();

	const masks = a.validity;
	try std.testing.expectEqual(@as(array.MaskInt, 0b1101), masks[0]);
	try std.testing.expectEqual(@as(T, 4), a.values_as(T)[3]);
}

test "varbinary init + deinit" {
	var b = try Builder([]const u8).init(std.testing.allocator);
	defer b.deinit();

	try b.append(&[_]u8{1,2,3});
}

test "varbinary utf8" {
	var b = try BuilderAdvanced([]const u8, .{ .is_large = true, .is_utf8 = true }).init(std.testing.allocator);
	defer b.deinit();

	try b.append(&[_]u8{1,2,3});
}

test "varbinary optional" {
	var b = try Builder(?[]const u8).init(std.testing.allocator);
	defer b.deinit();
	try b.append(null);
	try b.append(&[_]u8{1,2,3});

	const masks = b.validity.unmanaged.masks;
	try std.testing.expectEqual(@as(array.MaskInt, 0b10), masks[0]);
}

test "varbinary finish" {
	var b = try Builder(?[]const u8).init(std.testing.allocator);
	try b.append(null);
	try b.append("hello");

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(array.MaskInt, 0b10), a.validity[0]);
	try std.testing.expectEqualStrings("hello", a.values[0..5]);
}

test "c abi" {
	var b = try Builder(?[]const u8).init(std.testing.allocator);
	try b.append(null);
	try b.append("hello");

	var a = try b.finish();
	var c = try a.toOwnedAbi();
	defer c.release.?(@constCast(&c));

	const buf0 = @constCast(c.buffers.?[0].?);
	try std.testing.expectEqual(@as(array.MaskInt, 0b10), @ptrCast([*]u8, buf0)[0]);
	try std.testing.expectEqual(@as(i64, 1), c.null_count);

	const buf1 = @constCast(c.buffers.?[1].?);
	const indices = @ptrCast([*]i32, @alignCast(@alignOf(i32), buf1));
	try std.testing.expectEqual(@as(i32, 0), indices[0]);
	try std.testing.expectEqual(@as(i32, 0), indices[1]);
	try std.testing.expectEqual(@as(i32, 5), indices[2]);

	const buf2 = @constCast(c.buffers.?[2].?);
	const values = @ptrCast([*]u8, buf2);
	try std.testing.expectEqualStrings("hello", values[0..5]);
}
