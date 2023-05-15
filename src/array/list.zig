// List means single child. Includes List and Fixed-Size List layouts.
const std = @import("std");
const array = @import("./array.zig");
const tags = @import("../tags.zig");
const builder = @import("./builder.zig");

pub fn BuilderAdvanced(comptime ChildBuilder: type, comptime opts: tags.ListOptions, comptime fixed_len: i32) type {
	const NullCount = if (opts.is_nullable) usize else void;
	const ValidityList = if (opts.is_nullable) std.bit_set.DynamicBitSet else void;

	const OffsetType = if (opts.is_large) i64 else i32;
	const OffsetList = if (fixed_len == 0) std.ArrayListAligned(OffsetType, 64) else void;

	const ChildAppendType = ChildBuilder.Type();
	const AppendType = switch (opts.is_nullable) {
		true => if (fixed_len > 0) ?[fixed_len]ChildAppendType else ?[]const ChildAppendType,
		false => if (fixed_len > 0) [fixed_len]ChildAppendType else []const ChildAppendType,
	};

	return struct {
		const Self = @This();

		null_count: NullCount,
		validity: ValidityList,
		offsets: OffsetList,
		child: ChildBuilder,

		pub fn Type() type {
			return AppendType;
		}

		pub fn init(allocator: std.mem.Allocator) !Self {
			var res = Self {
				.null_count = if (NullCount != void) 0 else {},
				.validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
				.offsets = if (OffsetList != void) OffsetList.init(allocator) else {},
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
					if (OffsetList != void) {
						try self.offsets.append(self.offsets.items[self.offsets.items.len - 1]);
					} else {
						for (0..fixed_len) |_| {
							try self.child.append(0);
						}
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
				.Pointer => |p| switch (p.size) {
					.Slice => {
						for (value) |v| {
							try self.child.append(v);
						}
						try self.offsets.append(@intCast(OffsetType, self.child.values.items.len));
					},
					else => |t| @compileError("unsupported pointer type " ++ @tagName(t)),
				},
				.Array => |a| {
					std.debug.assert(a.len == fixed_len);
					for (value) |v| {
						try self.child.append(v);
					}
				},
				else => |t| @compileError("unsupported append type " ++ @tagName(t))
			};
		}

		pub fn append(self: *Self, value: AppendType) std.mem.Allocator.Error!void {
			return self.appendAny(value);
		}

		fn len(self: *Self) usize {
			if (OffsetList != void) {
				return self.offsets.items.len - 1;
			}
			return self.child.values.items.len / @intCast(usize, fixed_len);
		}

		pub fn finish(self: *Self) !*array.Array {
			const length = self.len();
			const allocator = self.child.values.allocator;
			const children = try allocator.alloc(*array.Array, 1);
			children[0] = try self.child.finish();
			const tag = if (fixed_len == 0)
				tags.Tag{ .list = opts }
			else tags.Tag { .list_fixed = .{
				.is_nullable = opts.is_nullable,
				.fixed_len = @intCast(i16, fixed_len),
				.is_large = opts.is_large
			} };

			var res = try array.Array.init(allocator);
			res.* = .{
				.tag = tag,
				.name = @typeName(AppendType) ++ " builder",
				.allocator = self.child.values.allocator,
				.length = length,
				.null_count = if (NullCount != void) self.null_count else 0,
				.validity = if (ValidityList != void) array.validity(&self.validity, self.null_count) else &[_]tags.MaskInt{},
				// TODO: implement @ptrCast between slices changing the length
				.offsets = if (OffsetList != void) std.mem.sliceAsBytes(try self.offsets.toOwnedSlice()) else &[_]u8{},
				.values = &.{},
				.children = children,
			};
			return res;
		}
	};
}

pub fn Builder(comptime Slice: type) type {
	const is_nullable = @typeInfo(Slice) == .Optional;
	const Child = if (is_nullable) @typeInfo(Slice).Optional.child else Slice;
	const t = @typeInfo(Child);
	if (!(t == .Pointer and t.Pointer.size == .Slice)) {
		@compileError(@typeName(Slice) ++ " is not a slice type");
	}
	const ChildBuilder = builder.Builder(t.Pointer.child);
	return BuilderAdvanced(ChildBuilder, .{ .is_nullable = is_nullable, .is_large = false }, 0);
}

test "init + deinit optional child and parent" {
	var b = try Builder([]const ?i8).init(std.testing.allocator);
	defer b.deinit();

	try b.append(&[_]?i8{1,null,3});
}

test "init + deinit varbinary" {
	var b = try Builder(?[][]const u8).init(std.testing.allocator);
	defer b.deinit();

	try b.append(null);
	try b.append(&[_][]const u8{"hello", "goodbye"});
}

test "finish" {
	var b = try Builder(?[]const i8).init(std.testing.allocator);
	try b.append(null);
	try b.append(&[_]i8{1,2,3});

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(array.MaskInt, 0b10), a.validity[0]);
}

test "abi" {
	var b = try Builder(?[]const i8).init(std.testing.allocator);
	try b.append(null);
	try b.append(&[_]i8{1,2,3});

	const a = try b.finish();

	var c = try a.toOwnedAbi();
	defer c.release.?(&c);
	var s = try a.ownedSchema();
	defer s.release.?(&s);
}
