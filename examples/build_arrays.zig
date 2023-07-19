const std = @import("std");
const arrow = @import("arrow");

const abi = arrow.abi;
const Builder = arrow.array.Builder;
const DictBuilder = arrow.array.dict.Builder;
const allocator = std.testing.allocator;

test "build arrays" {
    var b = try Builder(?i16).init(allocator);
    errdefer b.deinit();

    try b.append(null);
    try b.append(32);
    try b.append(33);
    try b.append(34);

    const array = try b.finish();
    defer array.deinit();
}

test "build dictionary array" {
    var b = try DictBuilder(?[]const u8).init(allocator);
    errdefer b.deinit();

    try b.appendNull();
    try b.append("hello");
    try b.append("there");
    try b.append("friend");

    const array = try b.finish();
    defer array.deinit();
}
