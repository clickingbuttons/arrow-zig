// Map means store a single child type named entries with fields (key, value)
// Useful for structures like [{"joe": 1}, {"blogs": 2, "foo": 4}, {}, null]
// Undocumented in official spec :)
const std = @import("std");
const array = @import("./array.zig");
const tags = @import("../tags.zig");
const builder = @import("./builder.zig");

const Array = array.Array;
const Allocator = std.mem.Allocator;

fn MakeTupleType(comptime KeyType: type, comptime ValueType: type) type {
	if (@typeInfo(KeyType) == .Optional) {
		@compileError("map key type '" ++ @typeName(KeyType) ++ "' cannot be nullable");
	}
 	return @Type(.{
 		.Struct = .{
 			.layout = .Auto,
 			.fields = &[2]std.builtin.Type.StructField{
				.{
					.name = "0",
					.type = KeyType,
					.default_value = null,
					.is_comptime = false,
					.alignment = 0,
				},
				.{
					.name = "1",
					.type = ValueType,
					.default_value = null,
					.is_comptime = false,
					.alignment = 0,
				},
			},
 			.decls = &.{},
 			.is_tuple = true,
 		},
 	});
}

pub fn BuilderAdvanced(
	comptime KeyBuilder: type,
	comptime ValueBuilder: type,
	comptime nullable: bool,
	comptime InTupleType: type,
) type {
	const NullCount = if (nullable) usize else void;
	const ValidityList = if (nullable) std.bit_set.DynamicBitSet else void;
	const OffsetList = std.ArrayListAligned(i32, Array.buffer_alignment);

	const KeyType = KeyBuilder.Type();
	const ValueType = ValueBuilder.Type();
	const Tuple = if (InTupleType != void) InTupleType else MakeTupleType(KeyType, ValueType);
	const AppendType = if (nullable) ?Tuple else Tuple;

	return struct {
		const Self = @This();

		allocator: Allocator,
		null_count: NullCount,
		validity: ValidityList,
		offsets: OffsetList,
		key_builder: KeyBuilder,
		value_builder: ValueBuilder,

		pub fn Type() type {
			return AppendType;
		}

		pub fn TupleType() type {
			return Tuple;
		}

		pub fn init(allocator: std.mem.Allocator) !Self {
			var key_builder = try KeyBuilder.init(allocator);
			errdefer key_builder.deinit();
			var value_builder = try ValueBuilder.init(allocator);
			errdefer value_builder.deinit();
			var res = Self {
				.allocator = allocator,
				.null_count = if (NullCount != void) 0 else {},
				.validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
				.offsets = OffsetList.init(allocator),
				.key_builder = key_builder,
				.value_builder = value_builder,
			};
			try res.offsets.append(0);

			return res;
		}

		pub fn deinit(self: *Self) void {
			if (ValidityList != void) self.validity.deinit();
			self.offsets.deinit();
			self.key_builder.deinit();
			self.value_builder.deinit();
		}

		inline fn appendValidity(self: *Self, is_valid: bool) Allocator.Error!void {
			if (ValidityList != void) try self.validity.resize(self.validity.capacity() + 1, is_valid);
		}

		inline fn appendTuple(self: *Self, tuple: Tuple) Allocator.Error!void {
			try self.key_builder.append(tuple[0]);
			try self.value_builder.append(tuple[1]);
		}

		pub fn append(self: *Self, value: AppendType) Allocator.Error!void {
			var offset = self.offsets.getLast();
			if (comptime nullable) {
				try self.appendValidity(value != null);
				if (value) |v| {
					try self.appendTuple(v);
					offset += 1;
				} else {
					self.null_count += 1;
				}
			} else {
				try self.appendTuple(value);
				offset += 1;
			}
			try self.offsets.append(offset);
		}

		pub fn appendSlice(self: *Self, kvs: []const Tuple) Allocator.Error!void {
			for (kvs) |kv| try self.appendTuple(kv);
			try self.appendValidity(true);
			try self.offsets.append(self.offsets.getLast() + @intCast(i32, kvs.len));
		}

		pub fn appendEmpty(self: *Self) Allocator.Error!void {
			try self.appendValidity(true);
			try self.offsets.append(self.offsets.getLast());
		}

		fn makeStruct(self: *Self) !*Array {
			const allocator = self.allocator;

			const children = try allocator.alloc(*Array, 2);
			children[0] = try self.key_builder.finish();
			children[0].name = "key";
			children[1] = try self.value_builder.finish();
			children[1].name = "value";

			var res = try Array.init(self.allocator);
			res.* = .{
				.tag = tags.Tag{ .Struct = .{ .nullable = false } },
				.name = "entries",
				.allocator = self.allocator,
				.length = self.offsets.items.len - 1 - self.null_count,
				.null_count = 0,
				.children = children,
			};
			return res;
		}

		pub fn finish(self: *Self) !*Array {
			// > A map<string, float64> array has format string +m; its single child has name entries and
			// > format string +s; its two grandchildren have names key and value, and format strings u
			// > and g respectively.
			const allocator = self.allocator;

			const children = try allocator.alloc(*Array, 1);
			children[0] = try self.makeStruct();

			var res = try Array.init(self.allocator);
			res.* = .{
				.tag = tags.Tag{ .Map = .{ .nullable = nullable } },
				.name = @typeName(AppendType) ++ " builder",
				.allocator = self.allocator,
				.length = self.offsets.items.len - 1,
				.null_count = if (NullCount != void) self.null_count else 0,
				.buffers = .{
					if (ValidityList != void)
						try array.validity(self.allocator, &self.validity, self.null_count)
					else &.{},
					std.mem.sliceAsBytes(try self.offsets.toOwnedSlice()),
					&.{},
				},
				.children = children,
			};
			return res;
		}
	};
}

const flat = @import("./flat.zig");

test "map advanced" {
	const B = BuilderAdvanced(flat.Builder([]const u8), flat.Builder(i32), false, void);
	const T = B.TupleType();
	var b = try B.init(std.testing.allocator);
	defer b.deinit();

	try b.append(T { "joe", 1 });
	try b.appendSlice(&[_]T{ .{"blogs", 2}, .{"foo", 4} });
	try b.appendEmpty();

	try std.testing.expectEqualSlices(i32, &[_]i32{0, 1, 3, 3}, b.offsets.items);
}

test "nullable map advanced with finish" {
	const B = BuilderAdvanced(flat.Builder([]const u8), flat.Builder(?i32), true, void);
	const T = B.TupleType();
	var b = try B.init(std.testing.allocator);

	try b.append(T { "joe", 1 });
	try b.appendSlice(&[_]T{ .{"blogs", 2}, .{"asdf", null} });
	try b.appendEmpty();
	try b.append(null);

	const a = try b.finish();
	defer a.deinit();

	const buffers = a.buffers;
	try std.testing.expectEqual(@as(u8, 0b0111), buffers[0][0]);
	try std.testing.expectEqualSlices(i32, &[_]i32{0, 1, 3, 3, 3}, std.mem.bytesAsSlice(i32, buffers[1]));
}

pub fn Builder(comptime Tuple: type) type {
	const nullable = @typeInfo(Tuple) == .Optional;
	const Child = if (nullable) @typeInfo(Tuple).Optional.child else Tuple;
	const t = @typeInfo(Child);
	if (t != .Struct or !t.Struct.is_tuple or t.Struct.fields.len != 2) {
		@compileError(@typeName(Tuple) ++ " is not a 2-element tuple type");
	}
	const KeyType = t.Struct.fields[0].type;
	if (@typeInfo(KeyType) == .Optional) {
		@compileError("map key type '" ++ @typeName(KeyType) ++ "' cannot be nullable");
	}
	const ValueType = t.Struct.fields[1].type;
	const KeyBuilder = builder.Builder(KeyType);
	const ValueBuilder = builder.Builder(ValueType);

	return BuilderAdvanced(KeyBuilder, ValueBuilder, nullable, Child);
}

test "init + deinit" {
	const T = struct { []const u8, i32 };
	var b = try Builder(T).init(std.testing.allocator);
	defer b.deinit();

	try b.append(.{ "hello", 1 });
	try b.append(T{ "goodbye", 2 });
}

test "finish" {
	const V = i32;
	const T = struct { []const u8, ?V };
	var b = try Builder(?T).init(std.testing.allocator);
	defer b.deinit();

	try b.append(null);
	try b.append(.{ "hello", 1 });
	try b.append(T{ "goodbye", 2 });
	try b.appendSlice(&[_]T{ .{ "arrow", 2 }, .{ "map", null } });

	const a = try b.finish();
	defer a.deinit();

	var buffers = a.buffers;
	try std.testing.expectEqual(@as(u8, 0b1110), buffers[0][0]);
	try std.testing.expectEqualSlices(i32, &[_]i32{0, 0, 1, 2, 4}, std.mem.bytesAsSlice(i32, buffers[1]));

	const child = a.children[0];
	// Child struct's key's values
	try std.testing.expectEqualStrings("hellogoodbyearrowmap", child.children[0].buffers[2]);
	// Child struct's values' validity
	try std.testing.expectEqual(@as(u8, 0b0111), child.children[1].buffers[0][0]);
	// Child struct's values' values
	try std.testing.expectEqualSlices(V, &[_]V{1,2,2,0}, std.mem.bytesAsSlice(V, child.children[1].buffers[1]));
}
