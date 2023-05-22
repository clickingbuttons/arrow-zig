//!
//! generated by flatc-zig
//! binary:     gen/format/Schema.bfbs
//! schema:     format/Schema.fbs
//! file ident: //Schema.fbs
//! typename    Field
//!

const std = @import("std");
const fb = @import("flatbufferz");
const Builder = fb.Builder;

const DictionaryEncoding = @import("DictionaryEncoding.fb.zig").DictionaryEncoding;
const DictionaryEncodingT = @import("DictionaryEncoding.fb.zig").DictionaryEncodingT;
const Type = @import("Type.fb.zig").Type;
const TypeT = @import("Type.fb.zig").TypeT;
const KeyValue = @import("KeyValue.fb.zig").KeyValue;
const KeyValueT = @import("KeyValue.fb.zig").KeyValueT;

/// ----------------------------------------------------------------------
/// A field represents a named column in a record / row batch or child of a
/// nested type.
pub const FieldT = struct {
    /// Name is not required, in i.e. a List
    name: []const u8 = "",
    /// Whether or not this field can contain nulls. Should be true in general.
    nullable: bool = false,
    /// This is the type of the decoded value if the field is dictionary encoded.
    type: TypeT = @intToEnum(Type.Tag, 0),
    /// Present only if the field is dictionary encoded.
    dictionary: ?*DictionaryEncodingT = null,
    /// children apply only to nested data types like Struct, List and Union. For
    /// primitive types children will have length 0.
    children: std.ArrayListUnmanaged(FieldT) = .{},
    /// User-defined metadata
    custom_metadata: std.ArrayListUnmanaged(KeyValueT) = .{},

    pub fn Pack(rcv: FieldT, __builder: *Builder, __pack_opts: fb.common.PackOptions) fb.common.PackError!u32 {
        _ = .{__pack_opts};
        var __tmp_offsets = std.ArrayListUnmanaged(u32){};
        defer if (__pack_opts.allocator) |alloc| __tmp_offsets.deinit(alloc);
        const name_off = if (rcv.name.len != 0) try __builder.createString(rcv.name) else 0;

        const type_off = try rcv.type.Pack(__builder, __pack_opts);

        const dictionary_off = if (rcv.dictionary) |x| try x.Pack(__builder, __pack_opts) else 0;

        var children_off: u32 = 0;
        if (rcv.children.items.len != 0) {
            const children_len = @intCast(i32, rcv.children.items.len);
            try __tmp_offsets.ensureTotalCapacity(__pack_opts.allocator.?, @bitCast(u32, children_len));
            __tmp_offsets.items.len = @bitCast(u32, children_len);
            for (__tmp_offsets.items, 0..) |*off, j| {
                off.* = try FieldT.Pack(rcv.children.items[j], __builder, __pack_opts);
            }
            _ = try Field.StartChildrenVector(__builder, children_len);
            {
                var j = children_len - 1;
                while (j >= 0) : (j -= 1) {
                    try __builder.prependUOff(__tmp_offsets.items[@bitCast(u32, j)]);
                }
                children_off = try __builder.endVector(@bitCast(u32, children_len));
            }
        }

        var custom_metadata_off: u32 = 0;
        if (rcv.custom_metadata.items.len != 0) {
            const custom_metadata_len = @intCast(i32, rcv.custom_metadata.items.len);
            try __tmp_offsets.ensureTotalCapacity(__pack_opts.allocator.?, @bitCast(u32, custom_metadata_len));
            __tmp_offsets.items.len = @bitCast(u32, custom_metadata_len);
            for (__tmp_offsets.items, 0..) |*off, j| {
                off.* = try KeyValueT.Pack(rcv.custom_metadata.items[j], __builder, __pack_opts);
            }
            _ = try Field.StartCustomMetadataVector(__builder, custom_metadata_len);
            {
                var j = custom_metadata_len - 1;
                while (j >= 0) : (j -= 1) {
                    try __builder.prependUOff(__tmp_offsets.items[@bitCast(u32, j)]);
                }
                custom_metadata_off = try __builder.endVector(@bitCast(u32, custom_metadata_len));
            }
        }

        try Field.Start(__builder);
        try Field.AddName(__builder, name_off);
        try Field.AddNullable(__builder, rcv.nullable);
        try Field.AddType_Type(__builder, rcv.type);
        try Field.AddType_(__builder, type_off);
        try Field.AddDictionary(__builder, dictionary_off);
        try Field.AddChildren(__builder, children_off);
        try Field.AddCustomMetadata(__builder, custom_metadata_off);
        return Field.End(__builder);
    }

    pub fn UnpackTo(rcv: Field, t: *FieldT, __pack_opts: fb.common.PackOptions) !void {
        _ = .{__pack_opts};
        t.name = rcv.Name();

        t.nullable = rcv.Nullable();

        if (rcv.Type_()) |_tab| {
            t.type = try TypeT.Unpack(rcv.Type_Type(), _tab, __pack_opts);
        }

        if (rcv.Dictionary()) |x| {
            if (t.dictionary == null) {
                t.dictionary = try __pack_opts.allocator.?.create(DictionaryEncodingT);
                t.dictionary.?.* = .{};
            }
            try DictionaryEncodingT.UnpackTo(x, t.dictionary.?, __pack_opts);
        }

        const children_len = rcv.ChildrenLen();
        t.children = try std.ArrayListUnmanaged(FieldT).initCapacity(__pack_opts.allocator.?, @bitCast(u32, children_len));
        t.children.expandToCapacity();
        {
            var j: u32 = 0;
            while (j < children_len) : (j += 1) {
                const x = rcv.Children(j).?;
                t.children.items[j] = try x.Unpack(__pack_opts);
            }
        }

        const custom_metadata_len = rcv.CustomMetadataLen();
        t.custom_metadata = try std.ArrayListUnmanaged(KeyValueT).initCapacity(__pack_opts.allocator.?, @bitCast(u32, custom_metadata_len));
        t.custom_metadata.expandToCapacity();
        {
            var j: u32 = 0;
            while (j < custom_metadata_len) : (j += 1) {
                const x = rcv.CustomMetadata(j).?;
                t.custom_metadata.items[j] = try x.Unpack(__pack_opts);
            }
        }
    }

    pub fn Unpack(rcv: Field, __pack_opts: fb.common.PackOptions) fb.common.PackError!FieldT {
        var t = FieldT{};
        try FieldT.UnpackTo(rcv, &t, __pack_opts);
        return t;
    }

    pub fn deinit(self: *FieldT, allocator: std.mem.Allocator) void {
        _ = .{ self, allocator };
        // TODO __pack_opts.dupe_strings
        // if(self.name.len > 0) allocator.free(self.name);
        self.type.deinit(allocator);
        if (self.dictionary) |x| {
            x.deinit(allocator);
            allocator.destroy(x);
        }
        for (self.children.items) |*it| it.deinit(allocator);
        self.children.deinit(allocator);
        for (self.custom_metadata.items) |*it| it.deinit(allocator);
        self.custom_metadata.deinit(allocator);
    }
};

pub const Field = struct {
    _tab: fb.Table,

    pub fn GetRootAs(buf: []u8, offset: u32) Field {
        const n = fb.encode.read(u32, buf[offset..]);
        return Field.init(buf, n + offset);
    }

    pub fn GetSizePrefixedRootAs(buf: []u8, offset: u32) Field {
        const n = fb.encode.read(u32, buf[offset + fb.Builder.size_u32 ..]);
        return Field.init(buf, n + offset + fb.Builder.size_u32);
    }

    pub fn init(bytes: []u8, pos: u32) Field {
        return .{ ._tab = .{ .bytes = bytes, .pos = pos } };
    }

    pub fn Table(x: Field) fb.Table {
        return x._tab;
    }

    /// Name is not required, in i.e. a List
    pub fn Name(rcv: Field) []const u8 {
        const o = rcv._tab.offset(4);
        if (o != 0) {
            return rcv._tab.byteVector(o + rcv._tab.pos);
        }
        return "";
    }

    /// Whether or not this field can contain nulls. Should be true in general.
    pub fn Nullable(rcv: Field) bool {
        const o = rcv._tab.offset(6);
        if (o != 0) {
            return rcv._tab.read(bool, o + rcv._tab.pos);
        }
        return false;
    }

    pub fn MutateNullable(rcv: Field, n: bool) bool {
        return rcv._tab.mutateSlot(bool, 6, n);
    }

    pub fn TypeType(rcv: Field) Type.Tag {
        const o = rcv._tab.offset(8);
        if (o != 0) {
            return rcv._tab.read(Type.Tag, o + rcv._tab.pos);
        }
        return @intToEnum(Type.Tag, 0);
    }

    pub fn MutateTypeType(rcv: Field, n: Type) bool {
        return rcv._tab.mutateSlot(Type, 8, n);
    }

    /// This is the type of the decoded value if the field is dictionary encoded.
    pub fn Type_(rcv: Field) ?fb.Table {
        const o = rcv._tab.offset(10);
        if (o != 0) {
            return rcv._tab.union_(o);
        }
        return null;
    }

    /// Present only if the field is dictionary encoded.
    pub fn Dictionary(rcv: Field) ?DictionaryEncoding {
        const o = rcv._tab.offset(12);
        if (o != 0) {
            const x = rcv._tab.indirect(o + rcv._tab.pos);
            return DictionaryEncoding.init(rcv._tab.bytes, x);
        }
        return null;
    }

    /// children apply only to nested data types like Struct, List and Union. For
    /// primitive types children will have length 0.
    pub fn Children(rcv: Field, j: usize) ?Field {
        const o = rcv._tab.offset(14);
        if (o != 0) {
            var x = rcv._tab.vector(o);
            x += @intCast(u32, j) * 4;
            x = rcv._tab.indirect(x);
            return Field.init(rcv._tab.bytes, x);
        }
        return null;
    }

    pub fn ChildrenLen(rcv: Field) u32 {
        const o = rcv._tab.offset(14);
        if (o != 0) {
            return rcv._tab.vectorLen(o);
        }
        return 0;
    }

    /// User-defined metadata
    pub fn CustomMetadata(rcv: Field, j: usize) ?KeyValue {
        const o = rcv._tab.offset(16);
        if (o != 0) {
            var x = rcv._tab.vector(o);
            x += @intCast(u32, j) * 4;
            x = rcv._tab.indirect(x);
            return KeyValue.init(rcv._tab.bytes, x);
        }
        return null;
    }

    pub fn CustomMetadataLen(rcv: Field) u32 {
        const o = rcv._tab.offset(16);
        if (o != 0) {
            return rcv._tab.vectorLen(o);
        }
        return 0;
    }

    pub fn Start(__builder: *Builder) !void {
        try __builder.startObject(7);
    }
    pub fn AddName(__builder: *Builder, name: u32) !void {
        try __builder.prependSlotUOff(0, name, 0);
    }

    pub fn AddNullable(__builder: *Builder, nullable: bool) !void {
        try __builder.prependSlot(bool, 1, nullable, false);
    }

    pub fn AddTypeType(__builder: *Builder, type_type: Type.Tag) !void {
        try __builder.prependSlot(Type.Tag, 2, type_type, @intToEnum(Type.Tag, 0));
    }

    pub fn AddType_(__builder: *Builder, @"type": u32) !void {
        try __builder.prependSlotUOff(3, @"type", 0);
    }

    pub fn AddDictionary(__builder: *Builder, dictionary: u32) !void {
        try __builder.prependSlotUOff(4, dictionary, 0);
    }

    pub fn AddChildren(__builder: *Builder, children: u32) !void {
        try __builder.prependSlotUOff(5, children, 0);
    }

    pub fn StartChildrenVector(__builder: *Builder, num_elems: i32) !u32 {
        return __builder.startVector(4, num_elems, 1);
    }
    pub fn AddCustomMetadata(__builder: *Builder, custom_metadata: u32) !void {
        try __builder.prependSlotUOff(6, custom_metadata, 0);
    }

    pub fn StartCustomMetadataVector(__builder: *Builder, num_elems: i32) !u32 {
        return __builder.startVector(4, num_elems, 1);
    }
    pub fn End(__builder: *Builder) !u32 {
        return __builder.endObject();
    }

    pub fn Unpack(rcv: Field, __pack_opts: fb.common.PackOptions) !FieldT {
        return FieldT.Unpack(rcv, __pack_opts);
    }
    pub fn FinishBuffer(__builder: *Builder, root: u32) !void {
        return __builder.Finish(root);
    }

    pub fn FinishSizePrefixedBuffer(__builder: *Builder, root: u32) !void {
        return __builder.FinishSizePrefixed(root);
    }
};
