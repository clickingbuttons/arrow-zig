const std = @import("std");
const arrow = @import("arrow");

const ipc = arrow.ipc;
const allocator = std.testing.allocator;

test "read file" {
    var ipc_reader = try ipc.reader.fileReader(allocator, "./testdata/tickers.arrow");
    defer ipc_reader.deinit();

    while (try ipc_reader.nextBatch()) |rb| {
        // Do something with rb
        defer rb.deinit();
    }
}

test "write file" {
    const batch = try arrow.sample.all(std.testing.allocator);
    try batch.toRecordBatch("record batch");
    defer batch.deinit();

    const fname = "./sample.arrow";
    var ipc_writer = try ipc.writer.fileWriter(std.testing.allocator, fname);
    defer ipc_writer.deinit();
    try ipc_writer.write(batch);
    try ipc_writer.finish();

    try std.fs.cwd().deleteFile(fname);
}
