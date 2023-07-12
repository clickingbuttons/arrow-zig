const std = @import("std");
const tags = @import("../tags.zig");
const Array = @import("../array/array.zig").Array;

const flat = @import("./gen/lib.zig");
pub const Schema = flat.Schema;
pub const Field = flat.Field;
pub const DictionaryEncoding = flat.DictionaryEncoding;
pub const Int = flat.Int;
pub const Float = flat.Float;
pub const Date = flat.Date;
pub const Time = flat.Time;
pub const Timestamp = flat.Timestamp;
pub const Duration = flat.Duration;
pub const Interval = flat.Interval;
pub const Union = flat.Union;
pub const FixedSizeBinary = flat.FixedSizeBinary;
pub const FixedSizeList = flat.FixedSizeList;
pub const Footer = flat.Footer;
pub const Message = flat.Message;
pub const RecordBatch = flat.RecordBatch;
pub const DictionaryBatch = flat.DictionaryBatch;
pub const BodyCompression = flat.BodyCompression;
pub const FieldNode = flat.FieldNode;
pub const FieldType = flat.Type;
pub const PackedFooter = flat.PackedFooter;
pub const PackedMessage = flat.PackedMessage;
pub const Type = flat.Type;
pub const MessageHeader = flat.MessageHeader;
pub const Buffer = flat.Buffer;
pub const Block = flat.Block;

pub const buffer_alignment = Array.buffer_alignment;
pub const magic = "ARROW1";
pub const MessageLen = i32;
pub const continuation: MessageLen = @bitCast(@as(u32, 0xffffffff));
pub const log = std.log.scoped(.arrow);

pub const IpcError = error{
    InvalidFieldTag,
    InvalidDictionaryIndexType,
    InvalidBitWidth,
    InvalidLen,
    NoSchema,
};
