const std = @import("std");
const Allocator = std.mem.Allocator;
const array_mod = @import("../array/array.zig");
const export_ = @import("export.zig");

// https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions
pub const Schema = extern struct {
    const Self = @This();

    pub const PrivateData = struct {
        allocator: Allocator,
        name_len: usize,
        abi_format_on_heap: bool,
    };

    format: [*:0]const u8, // Managed
    name: ?[*:0]const u8 = null, // Managed
    metadata: ?[*:0]const u8 = null, // Managed
    flags: packed struct(i64) {
        dictionary_ordered: bool = false,
        nullable: bool = false,
        map_keys_sorted: bool = false,
        _padding: u61 = 0,
    } = .{},
    n_children: i64 = 0,
    children: ?[*]*Schema = null, // Managed
    dictionary: ?*Schema = null, // Managed
    release: ?*const fn (*Schema) callconv(.C) void = null,
    private_data: ?*anyopaque = null,

    comptime {
        std.debug.assert(@sizeOf(@This()) == 72);
    }

    /// Creates a new abi.Schema from a abi.Array. Caller owns abi.Schema and must call `.release`.
    pub fn init(array: *array_mod.Array) !Self {
        return export_.schema.init(array);
    }

    pub fn deinit(self: *Self) void {
        if (self.release) |r| r(self);
        self.release = null;
    }
};

// https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
const ArrayLayout = enum {
    const Self = @This();

    Primitive,
    VariableBinary,
    List,
    FixedList,
    Struct,
    SparseUnion,
    DenseUnion,
    Null,
    Dictionary,
    Map, // Undocumented :)

    pub fn hasTypeIds(self: Self) bool {
        return switch (self) {
            .SparseUnion, .DenseUnion => true,
            else => false,
        };
    }

    pub fn hasValidity(self: Self) bool {
        return switch (self) {
            .Primitive, .VariableBinary, .List, .FixedList, .Struct, .Dictionary, .Map => true,
            else => false,
        };
    }

    pub fn hasOffsets(self: Self) bool {
        return switch (self) {
            .VariableBinary, .List, .DenseUnion, .Map => true,
            else => false,
        };
    }

    pub fn hasValues(self: Self) bool {
        return switch (self) {
            .Primitive, .VariableBinary, .Dictionary => true,
            else => false,
        };
    }

    pub fn nBuffers(self: Self) usize {
        var res: usize = 0;
        if (self.hasTypeIds()) res += 1;
        if (self.hasValidity()) res += 1;
        if (self.hasOffsets()) res += 1;
        if (self.hasValues()) res += 1;
        std.debug.assert(res <= 3);
        return res;
    }

    pub fn hasChildren(self: Self) bool {
        return switch (self) {
            .List, .FixedList, .Struct, .SparseUnion, .DenseUnion, .Dictionary, .Map => true,
            else => false,
        };
    }
};

pub const Array = extern struct {
    const Self = @This();
    pub const buffer_alignment = array_mod.Array.buffer_alignment;
    pub const Buffer = ?[*]align(buffer_alignment) const u8;
    pub const Buffers = ?[*]Buffer;
    pub const Layout = ArrayLayout;

    length: i64 = 0,
    null_count: i64 = 0,
    offset: i64 = 0,
    n_buffers: i64,
    n_children: i64 = 0,
    buffers: Buffers = null, // Managed
    children: ?[*]*Array = null, // Managed
    dictionary: ?*Array = null, // Managed
    release: ?*const fn (*Array) callconv(.C) void = null,
    private_data: ?*anyopaque = null,

    comptime {
        std.debug.assert(@sizeOf(@This()) == 80);
    }

    /// Moves array.Array into a new abi.Array. Caller owns abi.Array and must call `.release`.
    pub fn init(array: *array_mod.Array) !Self {
        return export_.array.init(array);
    }

    pub fn deinit(self: *Self) void {
        if (self.release) |r| r(self);
        self.release = null;
    }
};

fn testLayout(layout: Array.Layout, n_buffers: usize) !void {
    try std.testing.expectEqual(n_buffers, layout.nBuffers());
}
test "nbuffers" {
    // https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout
    try testLayout(Array.Layout.Primitive, 2);
    try testLayout(Array.Layout.VariableBinary, 3);
    try testLayout(Array.Layout.List, 2);
    try testLayout(Array.Layout.FixedList, 1);
    try testLayout(Array.Layout.Struct, 1);
    try testLayout(Array.Layout.SparseUnion, 1);
    try testLayout(Array.Layout.DenseUnion, 2);
    try testLayout(Array.Layout.Null, 0);
    try testLayout(Array.Layout.Dictionary, 2);
}

pub const ArrayStream = extern struct {
    get_schema: *const fn (*ArrayStream, *Schema) callconv(.C) c_int,
    get_next: *const fn (*ArrayStream, *Array) callconv(.C) c_int,
    get_last_error: *const fn (*ArrayStream) callconv(.C) [*]const u8,
    release: *const fn (*ArrayStream) callconv(.C) void,
    private_data: ?*anyopaque,

    comptime {
        std.debug.assert(@sizeOf(@This()) == 40);
    }
};
