// Dict means store indices in values to child array's values.
// I've decided that children should handle null entries instead of dicts.
const std = @import("std");
const array = @import("./array.zig");
const tags = @import("../tags.zig");
const builder = @import("./builder.zig");

// Context and max_load_percentage match std.HashMap.
pub fn BuilderAdvanced(
	comptime ChildBuilder: type,
	comptime opts: tags.DictOptions,
	comptime Context: type,
	comptime max_load_percentage: u64
) type {
	const IndexType = switch (opts.index) {
		.i32 => i32,
		.i16 => i16,
		.i8 => i8,
	};
	const IndexList = std.ArrayListAligned(IndexType, 64);

	const AppendType = ChildBuilder.Type();
	const HashMap = std.HashMap(AppendType, IndexType, Context, max_load_percentage);

	return struct {
		const Self = @This();

		indices: IndexList,
		hashmap: HashMap,
		child: ChildBuilder,

		pub fn Type() type {
			return AppendType;
		}

		pub fn init(allocator: std.mem.Allocator) !Self {
			return .{
				.indices = IndexList.init(allocator),
				.hashmap = HashMap.init(allocator),
				.child = try ChildBuilder.init(allocator),
			};
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

		pub fn finish(self: *Self) !*array.Array {
			const allocator = self.hashmap.allocator;
			const children = try allocator.alloc(*array.Array, 1);
			children[0] = try self.child.finish();
			self.hashmap.deinit();
			var res = try array.Array.init(allocator);
			res.* = .{
				.tag = tags.Tag{ .dictionary = opts },
				.name = @typeName(AppendType) ++ " builder",
				.allocator = allocator,
				.length = self.indices.items.len,
				.null_count = 0,
				.validity = &.{},
				.offsets = &.{},
				// TODO: implement @ptrCast between slices changing the length
				.values = std.mem.sliceAsBytes(try self.indices.toOwnedSlice()),
				.children = children,
			};
			return res;
		}
	};
}

pub fn getAutoHashFn(comptime K: type, comptime Context: type) (fn (Context, K) u64) {
	return struct {
		fn hash(ctx: Context, key: K) u64 {
			_ = ctx;
			var hasher = std.hash.Wyhash.init(0);
			std.hash.autoHashStrat(&hasher, key, .Deep); // Look at slice contents.
			return hasher.final();
		}
	}.hash;
}

pub fn getAutoEqlFn(comptime K: type, comptime Context: type) (fn (Context, K, K) bool) {
	return struct {
		fn eql(ctx: Context, a: K, b: K) bool {
			_ = ctx;
			// TODO: handle slices
			return std.meta.eql(a, b);
		}
	}.eql;
}


pub fn AutoContext(comptime K: type) type {
	return struct {
		pub const hash = getAutoHashFn(K, @This());
		pub const eql = getAutoEqlFn(K, @This());
	};
}

const flat = @import("./flat.zig");
test "init + deinit string" {
	const T = []const u8;
	var b = try BuilderAdvanced(
		flat.Builder(T),
		.{ .index = .i8 },
		AutoContext(T),
		std.hash_map.default_max_load_percentage
		).init(std.testing.allocator);
	defer b.deinit();

	try b.append("asdf");
	try b.append("ff");
	try b.append("asdf");
	try b.append("ff");
	try b.append("gg");

	try std.testing.expectEqual(@as(usize, 3), b.hashmap.count());
}

const list = @import("./list.zig");
test "init + deinit list" {
	const T = ?[]const []const u8;
	var b = try BuilderAdvanced(
		list.Builder(T),
		.{ .index = .i8 },
		AutoContext(T),
		std.hash_map.default_max_load_percentage
		).init(std.testing.allocator);
	defer b.deinit();

	try b.append(null);
	try b.append(&[_][]const u8{"hello", "goodbye"});

 	try std.testing.expectEqual(@as(usize, 2), b.hashmap.count());
}

const struct_ = @import("./struct.zig");
test "init + deinit struct" {
	const T = struct {
		a: ?i16,
		b: ?i32,
	};
	var b = try BuilderAdvanced(
		struct_.Builder(T),
		.{ .index = .i8 },
		AutoContext(T),
		std.hash_map.default_max_load_percentage
	).init(std.testing.allocator);
	defer b.deinit();

	try b.append(.{ .a = 4, .b = 1 });
	try b.append(T{ .a = 4, .b = 1 });
	try b.append(.{ .a = 4, .b = 2 });
	try b.append(T{ .a = 4, .b = 2 });

 	try std.testing.expectEqual(@as(usize, 2), b.hashmap.count());
}

test "finish" {
	const T = ?i8;
	var b = try BuilderAdvanced(
		flat.Builder(T),
		.{ .index = .i8 },
		AutoContext(T),
		std.hash_map.default_max_load_percentage
	).init(std.testing.allocator);
	try b.append(null);
	try b.append(1);

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(u8, 0), a.children[0].values[0]);
	try std.testing.expectEqual(@as(u8, 1), a.children[0].values[1]);
}

pub fn Builder(comptime T: type) type {
	return BuilderAdvanced(
		builder.Builder(T),
		.{ .index = .i16 },
		AutoContext(T),
		std.hash_map.default_max_load_percentage
	);
}

test "convienence finish" {
	const T = ?u8;
	var b = try Builder(T).init(std.testing.allocator);
	try b.append(null);
	try b.append(1);

	const a = try b.finish();
	defer a.deinit();

	try std.testing.expectEqual(@as(u8, 0), a.children[0].values[0]);
	try std.testing.expectEqual(@as(u8, 1), a.children[0].values[1]);
}
