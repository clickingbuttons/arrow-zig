// Dict means store indices in values to child array's values.
// I've decided that children should handle null entries instead of dicts.
const std = @import("std");
const array = @import("./array.zig");
const tags = @import("../tags.zig");

pub fn ArrayBuilderAdvanced(comptime ChildBuilder: type, comptime opts: tags.DictOptions) type {
	const IndexType = switch (opts.index) {
		.i64 => i64,
		.i32 => i32,
		.i16 => i16,
		.i8 => i8,
	};
	const IndexList = std.ArrayListAligned(IndexType, 64);

	const AppendType = ChildBuilder.Type();
	const HashMap = std.AutoHashMap(AppendType, IndexType);

	return struct {
		const Self = @This();

		indices: IndexList,
		hashmap: HashMap,
		child: ChildBuilder,

		pub fn init(allocator: std.mem.Allocator) !Self {
			var res = Self {
				.indices = IndexList.init(allocator),
				.hashmap = HashMap.init(allocator),
				.child = try ChildBuilder.init(allocator),
			};

			return res;
		}

		pub fn deinit(self: *Self) void {
			self.indices.deinit();
			self.hashmap.deinit();
			self.child.deinit();
		}

		pub fn append(self: *Self, value: AppendType) std.mem.Allocator.Error!void {
			const count = @intCast(IndexType, self.hashmap.count());
			const get_res = try self.hashmap.getOrPut(value);
			const index = if (get_res.found_existing) get_res.value_ptr.* else count;
			if (!get_res.found_existing) {
				get_res.value_ptr.* = count;
				try self.child.append(value);
			}
			try self.indices.append(index);
		}

		pub fn finish(self: *Self) !array.Array {
			const children = try self.child.values.allocator.alloc(array.Array, 1);
			children[0] = try self.child.finish();
			return .{
				.tag = tags.Tag{ .dictionary = opts },
				.allocator = self.child.values.allocator,
				.null_count = 0,
				// TODO: implement @ptrCast between slices changing the length
				.offsets = &.{},
				.values = std.mem.sliceAsBytes(try self.indices.toOwnedSlice()),
				.children = children,
			};
		}
	};
}

const flat = @import("./flat.zig");
// test "init + deinit string" {
// 	var b = try ArrayBuilderAdvanced(flat.ArrayBuilder([]const u8), .{ .index = .i8 }).init(std.testing.allocator);
// 	defer b.deinit();
// 
// 	try b.append("asdf");
// 	try b.append("ff");
// 	try b.append("asdf");
// 	try b.append("ff");
// 	try b.append("gg");
// 
//  	try std.testing.expectEqual(@as(usize, 3), b.hashmap.count());
// }

const struct_ = @import("./struct.zig");
test "init + deinit struct" {
	const T = struct {
		a: ?i16,
		b: ?i32,
	};
	var b = try ArrayBuilderAdvanced(struct_.ArrayBuilder(T), .{ .index = .i8 }).init(std.testing.allocator);
	defer b.deinit();

	try b.append(.{ .a = 4, .b = 1 });
	try b.append(T{ .a = 4, .b = 1 });
	try b.append(.{ .a = 4, .b = 2 });
	try b.append(T{ .a = 4, .b = 2 });

 	try std.testing.expectEqual(@as(usize, 2), b.hashmap.count());
}

// test "init + deinit varbinary" {
// 	var b = try ArrayBuilder(?[][]const u8).init(std.testing.allocator);
// 	defer b.deinit();
// 
// 	try b.append(null);
// 	try b.append(&[_][]const u8{"hello", "goodbye"});
// }
// 
// test "finish" {
// 	var b = try ArrayBuilder(?[]const i8).init(std.testing.allocator);
// 	try b.append(null);
// 	try b.append(&[_]i8{1,2,3});
// 
// 	const a = try b.finish();
// 	defer a.deinit();
// 
// 	try std.testing.expectEqual(@as(array.MaskInt, 0b10), a.validity[0]);
// }
