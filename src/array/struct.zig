// Struct means a child per field. Includes struct layout.
const std = @import("std");
const tags = @import("../tags.zig");
const flat = @import("./flat.zig");

fn MakeAppendType(comptime ChildrenBuilders: type, comptime is_nullable: bool) type {
	const t = @typeInfo(ChildrenBuilders).Struct;
 	var fields: [t.fields.len]std.builtin.Type.StructField = undefined;
 	for (t.fields, 0..) |f, i| {
		const ChildBuilderType = f.type.Type();
		if (is_nullable and @typeInfo(ChildBuilderType) != .Optional) {
			@compileError("'" ++ f.name ++ ": " ++ @typeName(ChildBuilderType) ++ "' is not nullable."
				++ " ALL nullable structs MUST be nullable");
		}
 		fields[i] = .{
 			.name = f.name,
 			.type = ChildBuilderType,
 			.default_value = null,
 			.is_comptime = false,
 			.alignment = 0,
		};
 	}
 	const T = @Type(.{
 		.Struct = .{
 			.layout = .Auto,
 			.fields = fields[0..],
 			.decls = &[_]std.builtin.Type.Declaration{},
 			.is_tuple = false,
 		},
 	});

	return if (is_nullable) ?T else T;
}

// ChildrenBuilders is a struct of { field_name: builder_type }
pub fn ArrayBuilderAdvanced(comptime ChildrenBuilders: type, comptime opts: tags.PrimitiveOptions, comptime StructType: type) type {
	const NullCount = if (opts.is_nullable) i64 else void;
	const ValidityList = if (opts.is_nullable) std.bit_set.DynamicBitSet else void;

	const AppendType = if (StructType != void) StructType else MakeAppendType(ChildrenBuilders, opts.is_nullable);

	return struct {
		const Self = @This();

		allocator: std.mem.Allocator,
		null_count: NullCount,
		validity: ValidityList,
		children: ChildrenBuilders,

		pub fn init(allocator: std.mem.Allocator) !Self {
			var children: ChildrenBuilders = undefined;
			inline for (@typeInfo(ChildrenBuilders).Struct.fields) |f| {
				const BuilderType = f.type;
				@field(children, f.name) = try BuilderType.init(allocator);
			}
			var res = Self {
				.allocator = allocator,
				.null_count = if (NullCount != void) 0 else {},
				.validity = if (ValidityList != void) try ValidityList.initEmpty(allocator, 0) else {},
				.children = children,
			};

			return res;
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

		fn numMasks(bit_length: usize) usize {
			return (bit_length + (@bitSizeOf(tags.MaskInt) - 1)) / @bitSizeOf(tags.MaskInt);
    }

		pub fn finish(self: *Self) !tags.Array {
			const fields = @typeInfo(ChildrenBuilders).Struct.fields;
			const children = try self.allocator.alloc(tags.Array, fields.len);
			inline for (fields, 0..) |f, i| {
				children[i] = try @field(self.children, f.name).finish();
			}
			return .{
				.tag = tags.Tag{ .struct_ = opts },
				.allocator = self.allocator,
				.null_count = if (NullCount != void) self.null_count else 0,
				.validity = if (ValidityList != void) self.validity.unmanaged.masks[0..numMasks(self.validity.unmanaged.bit_length)] else &[_]tags.MaskInt{},
				// TODO: implement @ptrCast between slices changing the length
				.offsets = &[_]u8{},
				.values = &[_]u8{},
				.children = children,
			};
		}
	};
}

test "struct advanced" {
	const ChildrenBuilders = struct {
		key: flat.ArrayBuilder([]const u8),
		val: flat.ArrayBuilder(i32),
	};
	var b = try ArrayBuilderAdvanced(ChildrenBuilders, .{ .is_nullable = false }, void).init(std.testing.allocator);
	defer b.deinit();

	try b.append(.{ .key = "asdf", .val = 1 });
}

test "nullable struct advanced with finish" {
	const ChildrenBuilders = struct {
		key: flat.ArrayBuilder(?[]const u8),
		val: flat.ArrayBuilder(?i32),
	};
	var b = try ArrayBuilderAdvanced(ChildrenBuilders, .{ .is_nullable = true }, void).init(std.testing.allocator);

	try b.append(null);
	try b.append(.{ .key = "asdf", .val = 1 });

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(tags.MaskInt, 0b10), a.validity[0]);
}

fn MakeChildrenBuilders(comptime Struct: type, comptime is_nullable: bool) type {
	const t = @typeInfo(Struct).Struct;
 	var fields: [t.fields.len]std.builtin.Type.StructField = undefined;
 	for (t.fields, 0..) |f, i| {
		if (is_nullable and @typeInfo(f.type) != .Optional) {
			@compileError("'" ++ f.name ++ ": " ++ @typeName(f.type) ++ "' is not nullable."
				++ " ALL nullable structs MUST be nullable");
		}
 		fields[i] = .{
 			.name = f.name,
 			.type = flat.ArrayBuilder(f.type),
 			.default_value = null,
 			.is_comptime = false,
 			.alignment = 0,
		};
 	}
 	return @Type(.{
 		.Struct = .{
 			.layout = .Auto,
 			.fields = fields[0..],
 			.decls = &[_]std.builtin.Type.Declaration{},
 			.is_tuple = false,
 		},
 	});
}

pub fn ArrayBuilder(comptime Struct: type) type {
	const is_nullable = @typeInfo(Struct) == .Optional;
	const Child = if (is_nullable) @typeInfo(Struct).Optional.child else Struct;
	const t = @typeInfo(Child);
	if (t != .Struct) {
		@compileError(@typeName(Struct) ++ " is not a struct type");
	}
	const ChildrenBuilders = MakeChildrenBuilders(Child, is_nullable);

	return ArrayBuilderAdvanced(ChildrenBuilders, .{ .is_nullable = is_nullable }, Struct);
}

test "init + deinit" {
	const T = struct {
		key: []const u8,
		val: i32,
	};
	var b = try ArrayBuilder(T).init(std.testing.allocator);
	defer b.deinit();

	try b.append(.{ .key = "hello", .val = 1 });
	try b.append(T{ .key = "goodbye", .val = 2 });
}

test "finish" {
	const T = struct {
		key: ?[]const u8,
		val: ?i32,
	};
	var b = try ArrayBuilder(?T).init(std.testing.allocator);

	try b.append(null);
	try b.append(.{ .key = "hello", .val = 1 });
	try b.append(T{ .key = "goodbye", .val = 2 });

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(tags.MaskInt, 0b110), a.validity[0]);
}
