const std = @import("std");
const flat = @import("./flat.zig");
const tags = @import("../tags.zig");
const Tag = tags.Tag;

const MaskInt = std.bit_set.DynamicBitSet.MaskInt;

pub fn Array(comptime is_nullable: bool, comptime is_large: bool) type {
	const tag = Tag.list;

	const NullCount = if (is_nullable) i64 else void;
	const ValidityList = if (is_nullable) []MaskInt else void;
	const OffsetType = if (is_large) i64 else i32;
	const OffsetSlice = []align(64) OffsetType;

	return struct {
		comptime tag: Tag = tag,

		allocator: std.mem.Allocator,
		null_count: NullCount,
		validity: ValidityList,
		offsets: OffsetSlice,
		child: tags.Array,

		const Self = @This();
		pub fn deinit(self: Self) void {
			self.allocator.free(self.offsets);
			// See bit_set.zig#deinit
			if (NullCount != void) {
				const old_allocation = (self.validity.ptr - 1)[0..(self.validity.ptr - 1)[0]];
				self.allocator.free(old_allocation);
			}
			// self.child.deinit();
		}
		pub fn nullCount(self: Self) i64 {
			return if (NullCount == void) 0 else self.null_count;
		}
	};
}

pub fn ArrayBuilder(comptime ChildBuilder: type, comptime is_nullable: bool, comptime is_large: bool) type {
	const NullCount = if (is_nullable) i64 else void;
	const ValidityList = if (is_nullable) std.bit_set.DynamicBitSet else void;

	const OffsetType = if (is_large) i64 else i32;
	const OffsetList = std.ArrayListAligned(OffsetType, 64);

	const ChildAppendType = ChildBuilder.Type();
	const AppendType = if (is_nullable) ?[]ChildAppendType else []ChildAppendType;

	return struct {
		const Self = @This();

		null_count: NullCount,
		validity: ValidityList,
		offsets: OffsetList,
		child: ChildBuilder,

		pub fn init(allocator: std.mem.Allocator) !Self {
			var res = Self {
				.null_count = if (NullCount != void) 0 else {},
				.validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
				.offsets = OffsetList.init(allocator),
				.child = try ChildBuilder.init(allocator),
			};
			if (OffsetList != void) {
				try res.offsets.append(0);
			}

			return res;
		}

		pub fn deinit(self: *Self) void {
			if (ValidityList != void) self.validity.deinit();
			if (OffsetList != void) self.offsets.deinit();
			self.child.deinit();
		}

		fn appendAny(self: *Self, value: anytype) std.mem.Allocator.Error!void {
			return switch (@typeInfo(@TypeOf(value))) {
				.Null => {
					try self.offsets.append(self.offsets.items[self.offsets.items.len - 1]);
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
				.Pointer => |p| switch (p.size) {
					.Slice => {
						for (value) |v| {
							try self.child.append(v);
						}
						try self.offsets.append(@intCast(OffsetType, self.child.values.items.len));
					},
					else => |t| @compileError("unsupported pointer type " ++ @tagName(t)),
				},
				else => |t| @compileError("unsupported append type " ++ @tagName(t))
			};
		}

		pub fn append(self: *Self, value: AppendType) std.mem.Allocator.Error!void {
			return self.appendAny(value);
		}

		fn numMasks(bit_length: usize) usize {
			return (bit_length + (@bitSizeOf(MaskInt) - 1)) / @bitSizeOf(MaskInt);
    }

		pub fn finish(self: *Self) !Array(is_nullable, is_large) {
			return .{
				.allocator = self.offsets.allocator,
				.null_count = self.null_count,
				.validity = if (ValidityList != void) self.validity.unmanaged.masks[0..numMasks(self.validity.unmanaged.bit_length)] else {},
				.offsets = try self.offsets.toOwnedSlice(),
				.child = tags.Array.init(try self.child.finish()),
			};
		}
	};
}

test "init + deinit optional child and parent" {
	var b = try ArrayBuilder(flat.ArrayBuilder(?i8, false, false), true, false).init(std.testing.allocator);
	defer b.deinit();

	try b.append(@constCast(&[_]?i8{1,null,3}));
	try b.append(null);
}

test "init + deinit varbinary" {
	var b = try ArrayBuilder(flat.ArrayBuilder([]u8, false, false), false, false).init(std.testing.allocator);
	defer b.deinit();

	try b.append(@constCast(&[_][]u8{@constCast(&[_]u8{1,2,3})}));
}

test "finish" {
	var b = try ArrayBuilder(flat.ArrayBuilder(i8, false, false), true, false).init(std.testing.allocator);
	try b.append(null);
	try b.append(@constCast(&[_]i8{1,2,3}));

	const a = try b.finish();
	defer a.deinit();

	const masks = a.validity;
	try std.testing.expectEqual(@as(MaskInt, 0b10), masks[0]);
}
