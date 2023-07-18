const std = @import("std");
const tags = @import("../tags.zig");

const Array = @import("../array/array.zig").Array;

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
