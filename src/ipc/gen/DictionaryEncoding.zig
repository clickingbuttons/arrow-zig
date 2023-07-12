//! generated by flatc-zig from Schema.fbs

const flatbuffers = @import("flatbuffers");
const types = @import("lib.zig");
const tags = @import("../../tags.zig");
const shared = @import("../shared.zig");

const log = shared.log;
const IpcError = shared.IpcError;

pub const DictionaryEncoding = struct {
    /// The known dictionary id in the application where this data is used. In
    /// the file or streaming formats, the dictionary ids are found in the
    /// DictionaryBatch messages
    id: i64 = 0,
    /// The dictionary indices are constrained to be non-negative integers. If
    /// this field is null, the indices must be signed int32. To maximize
    /// cross-language compatibility and performance, implementations are
    /// recommended to prefer signed integer types over unsigned integer types
    /// and to avoid uint64 indices unless they are required by an application.
    index_type: ?types.Int = null,
    /// By default, dictionaries are not ordered, or the order does not have
    /// semantic meaning. In some statistical, applications, dictionary-encoding
    /// is used to represent ordered categorical data, and we provide a way to
    /// preserve that metadata here
    is_ordered: bool = false,
    dictionary_kind: types.DictionaryKind = .dense_array,

    const Self = @This();

    pub fn init(packed_: PackedDictionaryEncoding) flatbuffers.Error!Self {
        const index_type_ = if (try packed_.indexType()) |i| try types.Int.init(i) else null;
        errdefer {}
        return .{
            .id = try packed_.id(),
            .index_type = index_type_,
            .is_ordered = try packed_.isOrdered(),
            .dictionary_kind = try packed_.dictionaryKind(),
        };
    }

    pub fn pack(self: Self, builder: *flatbuffers.Builder) flatbuffers.Error!u32 {
        const field_offsets = .{
            .index_type = if (self.index_type) |i| try i.pack(builder) else 0,
        };

        try builder.startTable();
        try builder.appendTableField(i64, self.id);
        try builder.appendTableFieldOffset(field_offsets.index_type);
        try builder.appendTableField(bool, self.is_ordered);
        try builder.appendTableField(types.DictionaryKind, self.dictionary_kind);
        return builder.endTable();
    }

    pub fn toTag(self: Self) IpcError!tags.Tag {
        if (self.index_type) |t| {
            const index: tags.DictOptions.Index = switch (t.bit_width) {
                8 => .i8,
                16 => .i16,
                32 => .i32,
                else => |w| {
                    log.err("dictionary {d} has invalid index bit width {d}", .{ self.id, w });
                    return IpcError.InvalidDictionaryIndexType;
                },
            };

            return .{ .Dictionary = .{ .index = index } };
        }

        log.err("dictionary {d} missing index type", .{self.id});
        return IpcError.InvalidDictionaryIndexType;
    }
};

pub const PackedDictionaryEncoding = struct {
    table: flatbuffers.Table,

    const Self = @This();

    pub fn init(size_prefixed_bytes: []u8) flatbuffers.Error!Self {
        return .{ .table = try flatbuffers.Table.init(size_prefixed_bytes) };
    }

    /// The known dictionary id in the application where this data is used. In
    /// the file or streaming formats, the dictionary ids are found in the
    /// DictionaryBatch messages
    pub fn id(self: Self) flatbuffers.Error!i64 {
        return self.table.readFieldWithDefault(i64, 0, 0);
    }

    /// The dictionary indices are constrained to be non-negative integers. If
    /// this field is null, the indices must be signed int32. To maximize
    /// cross-language compatibility and performance, implementations are
    /// recommended to prefer signed integer types over unsigned integer types
    /// and to avoid uint64 indices unless they are required by an application.
    pub fn indexType(self: Self) flatbuffers.Error!?types.PackedInt {
        return self.table.readField(?types.PackedInt, 1);
    }

    /// By default, dictionaries are not ordered, or the order does not have
    /// semantic meaning. In some statistical, applications, dictionary-encoding
    /// is used to represent ordered categorical data, and we provide a way to
    /// preserve that metadata here
    pub fn isOrdered(self: Self) flatbuffers.Error!bool {
        return self.table.readFieldWithDefault(bool, 2, false);
    }

    pub fn dictionaryKind(self: Self) flatbuffers.Error!types.DictionaryKind {
        return self.table.readFieldWithDefault(types.DictionaryKind, 3, .dense_array);
    }
};
