//! generated by flatc-zig from Schema.fbs

const flatbuffers = @import("flatbuffers");

/// Opaque binary data
pub const Binary = struct {
    const Self = @This();

    pub fn pack(self: Self, builder: *flatbuffers.Builder) flatbuffers.Error!u32 {
        _ = self;
        try builder.startTable();
        return builder.endTable();
    }
};

/// Opaque binary data
pub const PackedBinary = struct {};
