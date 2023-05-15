// Sparse + dense unions. Prefer dense.
const std = @import("std");
const tags = @import("../tags.zig");
const array = @import("./array.zig");
const builder = @import("./builder.zig");

// Per spec
const TypeId = i8;

// Gotta do this so we can "inline switch" on the field type
fn MakeEnumType(comptime ChildrenBuilders: type) type {
	const t = @typeInfo(ChildrenBuilders).Struct;
 	var fields: [t.fields.len]std.builtin.Type.EnumField = undefined;
 	for (t.fields, 0..) |f, i| {
 		fields[i] = .{
 			.name = f.name,
 			.value = i,
		};
 	}
 	return @Type(.{
 		.Enum = .{
			.tag_type = TypeId,
 			.fields = fields[0..],
 			.decls = &.{},
			.is_exhaustive = true,
 		},
 	});
}

fn MakeAppendType(comptime ChildrenBuilders: type, comptime is_nullable: bool) type {
	const t = @typeInfo(ChildrenBuilders).Struct;
 	var fields: [t.fields.len]std.builtin.Type.UnionField = undefined;
 	for (t.fields, 0..) |f, i| {
		const ChildBuilderType = f.type.Type();
		if (is_nullable and @typeInfo(ChildBuilderType) != .Optional) {
			@compileError("'" ++ f.name ++ ": " ++ @typeName(ChildBuilderType) ++ "' is not nullable."
				++ " ALL nullable structs MUST be nullable");
		}
 		fields[i] = .{
 			.name = f.name,
 			.type = ChildBuilderType,
 			.alignment = 0,
		};
 	}
 	const T = @Type(.{
 		.Union = .{
 			.layout = .Auto,
			.tag_type = MakeEnumType(ChildrenBuilders),
 			.fields = fields[0..],
 			.decls = &.{},
 		},
 	});

	return if (is_nullable) ?T else T;
}

pub fn BuilderAdvanced(comptime ChildrenBuilders: type, comptime opts: tags.UnionOptions, comptime UnionType: type) type {
	const AppendType = if (UnionType != void) UnionType else MakeAppendType(ChildrenBuilders, opts.is_nullable);
	const TypeList = std.ArrayListAligned(TypeId, 64);
	const OffsetList = std.ArrayListAligned(i32, 64);

	return struct {
		const Self = @This();

		allocator: std.mem.Allocator,
		types: TypeList,
		offsets: OffsetList,
		children: ChildrenBuilders,

		pub fn Type() type {
			return AppendType;
		}

		pub fn init(allocator: std.mem.Allocator) !Self {
			var children: ChildrenBuilders = undefined;
			inline for (@typeInfo(ChildrenBuilders).Struct.fields) |f| {
				const BuilderType = f.type;
				@field(children, f.name) = try BuilderType.init(allocator);
			}
			return .{
				.allocator = allocator,
				.types = TypeList.init(allocator),
				.offsets = OffsetList.init(allocator),
				.children = children,
			};
		}

		pub fn deinit(self: *Self) void {
			self.types.deinit();
			self.offsets.deinit();
			inline for (@typeInfo(ChildrenBuilders).Struct.fields) |f| {
				@field(self.children, f.name).deinit();
			}
		}

		fn appendAny(self: *Self, value: anytype) std.mem.Allocator.Error!void {
			return switch (@typeInfo(@TypeOf(value))) {
				.Null => {
					if (opts.is_dense) {
						const num = 0;
						const first_field = @typeInfo(ChildrenBuilders).Struct.fields[num];
						var child_builder = &@field(self.children, first_field.name);

						try self.types.append(num);
						try self.offsets.append(@intCast(i32, child_builder.values.items.len));
						try child_builder.append(null);
					} else {
						inline for (@typeInfo(ChildrenBuilders).Struct.fields) |f| {
							try @field(self.children, f.name).append(null);
						}
					}
				},
				.Optional => {
					if (value) |v| {
						try self.appendAny(v);
					} else {
						try self.appendAny(null);
					}
				},
				.Union => switch(value) {
					inline else => |_, tag| {
						if (opts.is_dense) {
							var child_builder = &@field(self.children, @tagName(tag));

							try self.types.append(@enumToInt(tag));
							try self.offsets.append(@intCast(i32, child_builder.values.items.len));
							try child_builder.append(@field(value, @tagName(tag)));
						} else {
							inline for (@typeInfo(ChildrenBuilders).Struct.fields, 0..) |f, i| {
								const to_append = if (i == @enumToInt(tag)) @field(value, f.name) else null;
								try @field(self.children, f.name).append(to_append);
							}
						}
					}
				},
				else => |t| @compileError("unsupported append type " ++ @tagName(t))
			};
		}

		pub fn append(self: *Self, value: AppendType) std.mem.Allocator.Error!void {
			return self.appendAny(value);
		}

		pub fn finish(self: *Self) !*array.Array {
			const fields = @typeInfo(ChildrenBuilders).Struct.fields;
			const children = try self.allocator.alloc(*array.Array, fields.len);
			inline for (fields, 0..) |f, i| {
				children[i] = try @field(self.children, f.name).finish();
			}
			var res = try array.Array.init(self.allocator);
			res.* = .{
				.tag = tags.Tag{ .union_ = opts },
				.name = @typeName(AppendType) ++ " builder",
				.allocator = self.allocator,
				.length = self.types.items.len,
				.null_count = 0,
				.validity = &.{},
				// TODO: implement @ptrCast between slices changing the length
				.offsets = std.mem.sliceAsBytes(try self.offsets.toOwnedSlice()),
				.values = std.mem.sliceAsBytes(try self.types.toOwnedSlice()),
				.children = children,
			};
			return res;
		}
	};
}

const flat = @import("./flat.zig");

test "union advanced" {
	const ChildrenBuilders = struct {
		key: flat.Builder([]const u8),
		val: flat.Builder(i32),
	};
	var b = try BuilderAdvanced(ChildrenBuilders, .{ .is_nullable = false, .is_dense = true }, void).init(std.testing.allocator);
	defer b.deinit();

	try b.append(.{ .key = "asdf" });
	try b.append(.{ .val = 32 });
}

test "nullable union advanced with finish" {
	// Straight from example
	const ChildrenBuilders = struct {
		f: flat.Builder(?f32),
		i: flat.Builder(?i32),
	};
	var b = try BuilderAdvanced(ChildrenBuilders, .{ .is_nullable = true, .is_dense = true }, void).init(std.testing.allocator);

	try b.append(.{ .f = 1.2 });
	try b.append(null);
	try b.append(.{ .f = 3.4 });
	try b.append(.{ .i = 5 });

	var a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(u8, 0), a.values[0]);
	try std.testing.expectEqual(@as(u8, 0), a.values[1]);
	try std.testing.expectEqual(@as(u8, 0), a.values[2]);
	try std.testing.expectEqual(@as(u8, 1), a.values[3]);
	try std.testing.expectEqual(@as(array.MaskInt, 0b0101), a.children[0].validity[0]);
	try std.testing.expectEqual(@as(usize, 0), a.children[1].validity.len);
}

test "nullable union advanced with abi finish" {
	// Straight from example
	const ChildrenBuilders = struct {
		f: flat.Builder(?f32),
		i: flat.Builder(?i32),
	};
	var b = try BuilderAdvanced(ChildrenBuilders, .{ .is_nullable = true, .is_dense = true }, void).init(std.testing.allocator);

	try b.append(.{ .f = 1.2 });
	try b.append(null);
	try b.append(.{ .f = 3.4 });
	try b.append(.{ .i = 5 });

	var a = try b.finish();
	var arr = try a.toOwnedAbi();
	defer arr.release.?(@constCast(&arr));

	try std.testing.expectEqual(@as(i64, 2), arr.n_buffers);
	try std.testing.expectEqual(@as(i64, 2), arr.n_children);
	const values = @ptrCast([*]const u8, arr.buffers.?[0].?);
	const validity = @ptrCast([*]const u8, arr.children.?[0].buffers.?[0].?);
	try std.testing.expectEqual(@as(u8, 0), values[0]);
	try std.testing.expectEqual(@as(u8, 0), values[1]);
	try std.testing.expectEqual(@as(u8, 0), values[2]);
	try std.testing.expectEqual(@as(u8, 1), values[3]);
	try std.testing.expectEqual(@as(u8, 0b0101), validity[0]);

	const s = try a.ownedSchema();
	defer s.release.?(@constCast(&s));

	try std.testing.expectEqualStrings("+ud:0,1\x00", s.format[0..8]);
}

test "nullable sparse union advanced with finish" {
	// Straight from example + an extra null
	const ChildrenBuilders = struct {
		i: flat.Builder(?i32),
		f: flat.Builder(?f32),
		s: flat.Builder(?[]const u8),
	};
	var b = try BuilderAdvanced(ChildrenBuilders, .{ .is_nullable = true, .is_dense = false }, void).init(std.testing.allocator);

	try b.append(null);
	try b.append(.{ .i = 5 });
	try b.append(.{ .f = 1.2 });
	try b.append(.{ .s = "joe" });
	try b.append(.{ .f = 3.4 });
	try b.append(.{ .i = 4 });
	try b.append(.{ .s = "mark" });

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(array.MaskInt, 0b0100010), a.children[0].validity[0]);
	try std.testing.expectEqual(@as(array.MaskInt, 0b0010100), a.children[1].validity[0]);
	try std.testing.expectEqual(@as(array.MaskInt, 0b1001000), a.children[2].validity[0]);
}

fn MakeChildrenBuilders(comptime Union: type, comptime is_nullable: bool) type {
	const t = @typeInfo(Union).Union;
 	var fields: [t.fields.len]std.builtin.Type.StructField = undefined;
 	for (t.fields, 0..) |f, i| {
		if (is_nullable and @typeInfo(f.type) != .Optional) {
			@compileError("'" ++ f.name ++ ": " ++ @typeName(f.type) ++ "' is not nullable."
				++ " ALL nullable union fields MUST be nullable");
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

pub fn Builder(comptime Union: type) type {
	const is_nullable = @typeInfo(Union) == .Optional;
	const Child = if (is_nullable) @typeInfo(Union).Optional.child else Union;
	const t = @typeInfo(Child);
	if (t != .Union) {
		@compileError(@typeName(Union) ++ " is not a union type");
	}
	const ChildrenBuilders = MakeChildrenBuilders(Child, is_nullable);

	return BuilderAdvanced(ChildrenBuilders, .{ .is_nullable = is_nullable, .is_dense = true }, Union);
}

test "init + deinit" {
	const T = union(enum) {
		key: []const u8,
		val: i32,
	};
	var b = try Builder(T).init(std.testing.allocator);
	defer b.deinit();

	try b.append(.{ .key = "hello" });
	try b.append(T{ .val = 2 });
}
