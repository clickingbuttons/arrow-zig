//!
//! generated by flatc-zig
//! binary:     gen/format/Schema.bfbs
//! schema:     format/Schema.fbs
//! file ident: //Schema.fbs
//! typename    DictionaryKind
//!

const std = @import("std");
const fb = @import("flatbufferz");
const Builder = fb.Builder;

/// ----------------------------------------------------------------------
/// Dictionary encoding metadata
/// Maintained for forwards compatibility, in the future
/// Dictionaries might be explicit maps between integers and values
/// allowing for non-contiguous index values
pub const DictionaryKind = enum(i16) {
    DenseArray = 0,
    pub fn tagName(v: @This()) []const u8 {
        return @tagName(v);
    }
};