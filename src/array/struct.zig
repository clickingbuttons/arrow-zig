// Struct means a child per field. Includes struct layout.
const std = @import("std");
const tags = @import("../tags.zig");
const array = @import("./array.zig");
const flat = @import("./flat.zig");
const builder = @import("./builder.zig");

const Array = array.Array;

fn MakeStructType(comptime ChildrenBuilders: type, comptime nullable: bool) type {
	const t = @typeInfo(ChildrenBuilders).Struct;
 	var fields: [t.fields.len]std.builtin.Type.StructField = undefined;
 	for (t.fields, 0..) |f, i| {
		const ChildBuilderType = f.type.Type();
		if (nullable and @typeInfo(ChildBuilderType) != .Optional) {
			@compileError("'" ++ f.name ++ ": " ++ @typeName(ChildBuilderType) ++ "' is not nullable."
				++ " ALL nullable struct fields MUST be nullable"
				++ " so that `.append(null)` can append null to each field");
		}
 		fields[i] = .{
 			.name = f.name,
 			.type = ChildBuilderType,
 			.default_value = null,
 			.is_comptime = false,
 			.alignment = 0,
		};
 	}
 	return @Type(.{
 		.Struct = .{
 			.layout = .Auto,
 			.fields = fields[0..],
 			.decls = &.{},
 			.is_tuple = false,
 		},
 	});
}

// ChildrenBuilders is a struct of { field_name: builder_type }
pub fn BuilderAdvanced(
	comptime ChildrenBuilders: type,
	comptime nullable: bool,
	comptime InStructType: type, // Needed because there is no struct -> struct type coercion
) type {
	const NullCount = if (nullable) usize else void;
	const ValidityList = if (nullable) std.bit_set.DynamicBitSet else void;

	const Struct = if (InStructType != void)
		InStructType
	else
		MakeStructType(ChildrenBuilders, nullable);

	const AppendType = if (nullable) ?Struct else Struct;

	return struct {
		const Self = @This();

		allocator: std.mem.Allocator,
		null_count: NullCount,
		validity: ValidityList,
		children: ChildrenBuilders,

		pub fn Type() type {
			return AppendType;
		}

		pub fn StructType() type {
			return Struct;
		}

		pub fn init(allocator: std.mem.Allocator) !Self {
			var children: ChildrenBuilders = undefined;
			inline for (@typeInfo(ChildrenBuilders).Struct.fields) |f| {
				const BuilderType = f.type;
				@field(children, f.name) = try BuilderType.init(allocator);
			}
			return .{
				.allocator = allocator,
				.null_count = if (NullCount != void) 0 else {},
				.validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
				.children = children,
			};
		}

		pub fn deinit(self: *Self) void {
			if (ValidityList != void) self.validity.deinit();
			inline for (@typeInfo(ChildrenBuilders).Struct.fields) |f| {
				@field(self.children, f.name).deinit();
			}
		}

		fn appendAny(self: *Self, value: anytype) std.mem.Allocator.Error!void {
			return switch (@typeInfo(@TypeOf(value))) {
				.Null => {
					inline for (@typeInfo(ChildrenBuilders).Struct.fields) |f| {
						try @field(self.children, f.name).append(null);
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
				.Struct => {
					inline for (@typeInfo(ChildrenBuilders).Struct.fields) |f| {
						try @field(self.children, f.name).append(@field(value, f.name));
					}
				},
				else => |t| @compileError("unsupported append type " ++ @tagName(t))
			};
		}

		pub fn append(self: *Self, value: AppendType) std.mem.Allocator.Error!void {
			return self.appendAny(value);
		}

		pub fn finish(self: *Self) !*Array {
			const fields = @typeInfo(ChildrenBuilders).Struct.fields;
			const children = try self.allocator.alloc(*Array, fields.len);
			inline for (fields, 0..) |f, i| {
				children[i] = try @field(self.children, f.name).finish();
				children[i].name = f.name;
			}
			var res = try array.Array.init(self.allocator);
			res.* = .{
				.tag = tags.Tag{ .Struct = .{ .nullable = nullable } },
				.name = @typeName(AppendType) ++ " builder",
				.allocator = self.allocator,
				.length = children[0].length,
				.null_count = if (NullCount != void) self.null_count else 0,
				.buffers = .{
					if (ValidityList != void)
						try array.validity(self.allocator, &self.validity, self.null_count)
					else &.{},
					&.{},
					&.{},
				},
				.children = children,
			};
			return res;
		}
	};
}

test "struct advanced" {
	const ChildrenBuilders = struct {
		key: flat.Builder([]const u8),
		val: flat.Builder(i32),
	};
	const B = BuilderAdvanced(ChildrenBuilders, false, void);
	const T = B.StructType();
	var b = try B.init(std.testing.allocator);
	defer b.deinit();

	try b.append(T{ .key = "asdf", .val = 1 });
	try b.append(.{ .key = "ffgg", .val = 2 });
}

test "nullable struct advanced with finish" {
	const ChildrenBuilders = struct {
		key: flat.Builder(?[]const u8),
		val: flat.Builder(?i32),
	};
	const B = BuilderAdvanced(ChildrenBuilders, true, void);
	const T = B.StructType();
	var b = try B.init(std.testing.allocator);

	try b.append(null);
	try b.append(T{ .key = "asdf", .val = 1 });
	try b.append(.{ .key = "asdf", .val = null });

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(u8, 0b110), a.buffers[0][0]);
	try std.testing.expectEqual(@as(u8, 0b010), a.children[1].buffers[0][0]);
}

fn MakeChildrenBuilders(comptime Struct: type, comptime nullable: bool) type {
	const t = @typeInfo(Struct).Struct;
 	var fields: [t.fields.len]std.builtin.Type.StructField = undefined;
 	for (t.fields, 0..) |f, i| {
		if (nullable and @typeInfo(f.type) != .Optional) {
			@compileError("'" ++ f.name ++ ": " ++ @typeName(f.type) ++ "' is not nullable."
				++ " ALL nullable struct fields MUST be nullable"
				++ " so that `.append(null)` can append null to each field");
		}
 		fields[i] = .{
 			.name = f.name,
 			.type = builder.Builder(f.type),
 			.default_value = null,
 			.is_comptime = false,
 			.alignment = 0,
		};
 	}
 	return @Type(.{
 		.Struct = .{
 			.layout = .Auto,
 			.fields = fields[0..],
 			.decls = &.{},
 			.is_tuple = false,
 		},
 	});
}

pub fn Builder(comptime Struct: type) type {
	const nullable = @typeInfo(Struct) == .Optional;
	const Child = if (nullable) @typeInfo(Struct).Optional.child else Struct;
	const t = @typeInfo(Child);
	if (t != .Struct) {
		@compileError(@typeName(Struct) ++ " is not a struct type");
	}
	const ChildrenBuilders = MakeChildrenBuilders(Child, nullable);

	return BuilderAdvanced(ChildrenBuilders, nullable, Child);
}

test "init + deinit" {
	const T = struct {
		key: []const u8,
		val: i32,
	};
	var b = try Builder(T).init(std.testing.allocator);
	defer b.deinit();

	try b.append(.{ .key = "hello", .val = 1 });
	try b.append(T{ .key = "goodbye", .val = 2 });
}

test "finish" {
	const T = struct {
		key: ?[]const u8,
		val: ?i32,
	};
	var b = try Builder(?T).init(std.testing.allocator);

	try b.append(null);
	try b.append(.{ .key = "hello", .val = 1 });
	try b.append(T{ .key = "goodbye", .val = 2 });

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(u8, 0b110), a.buffers[0][0]);
}
