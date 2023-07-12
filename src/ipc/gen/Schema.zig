//! generated by flatc-zig from Schema.fbs

const flatbuffers = @import("flatbuffers");
const std = @import("std");
const types = @import("lib.zig");
const IpcError = @import("../shared.zig").IpcError;

/// ----------------------------------------------------------------------
/// A Schema describes the columns in a row batch
pub const Schema = struct {
    /// endianness of the buffer
    /// it is Little Endian by default
    /// if endianness doesn't match the underlying system then the vectors need to be converted
    endianness: types.Endianness = .little,
    fields: []types.Field,
    custom_metadata: []types.KeyValue,
    /// Features used in the stream/file.
    features: []types.Feature,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, packed_: PackedSchema) flatbuffers.Error!Self {
        const fields_ = try flatbuffers.unpackVector(allocator, types.Field, packed_, "fields");
        errdefer {
            for (fields_) |f| f.deinit(allocator);
            allocator.free(fields_);
        }
        const custom_metadata_ = try flatbuffers.unpackVector(allocator, types.KeyValue, packed_, "customMetadata");
        errdefer {
            for (custom_metadata_) |c| c.deinit(allocator);
            allocator.free(custom_metadata_);
        }
        const features_ = try flatbuffers.unpackVector(allocator, types.Feature, packed_, "features");
        errdefer {
            allocator.free(features_);
        }
        return .{
            .endianness = try packed_.endianness(),
            .fields = fields_,
            .custom_metadata = custom_metadata_,
            .features = features_,
        };
    }

    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        for (self.fields) |f| f.deinit(allocator);
        allocator.free(self.fields);
        for (self.custom_metadata) |c| c.deinit(allocator);
        allocator.free(self.custom_metadata);
        allocator.free(self.features);
    }

    pub fn pack(self: Self, builder: *flatbuffers.Builder) flatbuffers.Error!u32 {
        const field_offsets = .{
            .fields = try builder.prependVectorOffsets(types.Field, self.fields),
            .custom_metadata = try builder.prependVectorOffsets(types.KeyValue, self.custom_metadata),
            .features = try builder.prependVector(types.Feature, self.features),
        };

        try builder.startTable();
        try builder.appendTableField(types.Endianness, self.endianness);
        try builder.appendTableFieldOffset(field_offsets.fields);
        try builder.appendTableFieldOffset(field_offsets.custom_metadata);
        try builder.appendTableFieldOffset(field_offsets.features);
        return builder.endTable();
    }

    pub fn nFields(self: Self) usize {
        var res: usize = 0;
        for (self.fields) |field| res += 1 + field.nFields();
        return res;
    }

    pub fn nBuffers(self: Self) IpcError!usize {
        var res: usize = 0;
        for (self.fields) |field| res += try field.nBuffers();
        return res;
    }
};

/// ----------------------------------------------------------------------
/// A Schema describes the columns in a row batch
pub const PackedSchema = struct {
    table: flatbuffers.Table,

    const Self = @This();

    pub fn init(size_prefixed_bytes: []u8) flatbuffers.Error!Self {
        return .{ .table = try flatbuffers.Table.init(size_prefixed_bytes) };
    }

    /// endianness of the buffer
    /// it is Little Endian by default
    /// if endianness doesn't match the underlying system then the vectors need to be converted
    pub fn endianness(self: Self) flatbuffers.Error!types.Endianness {
        return self.table.readFieldWithDefault(types.Endianness, 0, .little);
    }

    pub fn fieldsLen(self: Self) flatbuffers.Error!u32 {
        return self.table.readFieldVectorLen(1);
    }
    pub fn fields(self: Self, index: usize) flatbuffers.Error!types.PackedField {
        return self.table.readFieldVectorItem(types.PackedField, 1, index);
    }

    pub fn customMetadataLen(self: Self) flatbuffers.Error!u32 {
        return self.table.readFieldVectorLen(2);
    }
    pub fn customMetadata(self: Self, index: usize) flatbuffers.Error!types.PackedKeyValue {
        return self.table.readFieldVectorItem(types.PackedKeyValue, 2, index);
    }

    /// Features used in the stream/file.
    pub fn features(self: Self) flatbuffers.Error![]align(1) types.Feature {
        return self.table.readField([]align(1) types.Feature, 3);
    }
};
