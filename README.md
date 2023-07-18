# arrow-zig

![zig-version](https://img.shields.io/badge/dynamic/yaml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fclickingbuttons%2Farrow-zig%2Fmaster%2F.github%2Fworkflows%2Ftest.yml&query=%24.jobs.test.steps%5B1%5D.with.version&label=zig-version)
![tests](https://github.com/clickingbuttons/arrow-zig/actions/workflows/test.yml/badge.svg)
[![docs](https://github.com/clickingbuttons/arrow-zig/actions/workflows/publish_docs.yml/badge.svg)](https://clickingbuttons.github.io/arrow-zig)

Library to build Arrow arrays from Zig primitives and read/write them to FFI and IPC formats.

## Installation

`build.zig.zon`
```zig
.{
    .name = "yourProject",
    .version = "0.0.1",

    .dependencies = .{
        .@"arrow-zig" = .{
            .url = "https://github.com/clickingbuttons/arrow-zig/archive/refs/tags/latest-release.tar.gz",
        },
    },
}
```

`build.zig`
```zig
const arrow_dep = b.dependency("arrow-zig", .{
    .target = target,
    .optimize = optimize,
});
your_lib_or_exe.addModule("arrow", arrow_dep.module("arrow"));
```

Run `zig build` and then copy the expected hash into `build.zig.zon`.

## Usage

Arrow has [11 different array types](https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout). Here's how arrow-zig maps them to Zig types.

| Arrow type            | Zig type                                  | arrow-zig builder |
|-----------------------|-------------------------------------------|-------------------|
| Primitive             | i{8,16,32,64}, u{8,16,32,64}, f{16,32,64} | flat              |
| Variable binary       | []u8,[]const u8                           | flat              |
| List                  | []T                                       | list              |
| Fixed-size list       | [N]T                                      | list              |
| Struct                | struct                                    | struct            |
| Dense union (default) | union                                     | union             |
| Sparse union          | union                                     | union             |
| Null                  | void                                      | Array.null_array  |
| Dictionary            | T                                         | dictionary        |
| Map                   | struct { T, V }, struct { T, ?V }         | map               |
| Run-end encoded       | N/A                                       | N/A               |

Notes:

1. Run-end encoded array compression can be acheived by LZ4. Use that instead.
2. There is currently no Decimal type or library in Zig. Once added it will be a primitive.

### Build arrays

The default `Builder` can map Zig types with reasonable defaults except for Dictionary types. You can import it like this:
```zig
const Builder = @import("arrow").array.Builder;
```

```zig
var b = try Builder(?i16).init(allocator);
try b.append(null);
try b.append(32);
try b.append(33);
try b.append(34);
```

Null-safety is preserved at compile time.
```zig
var b = try Builder(i16).init(allocator);
try b.append(null);
```
...
```
error: expected type 'i16', found '@TypeOf(null)'
    try b.append(null);
```

Dictionary types must use an explicit builder.
```zig
const DictBuilder = @import("arrow").array.dict.Builder;
```

```zig
var b = try DictBuilder(?[]const u8).init(allocator);
try b.appendNull();
try b.append("hello");
try b.append("there");
try b.append("friend");
```

You can customize exactly how the type maps to Arrow with each type's `BuilderAdvanced`. For example to build a sparse union of structs:
```zig
var b = try UnionBuilder(
    struct {
        f: Builder(?f32),
        i: Builder(?i32),
    },
    .{ .nullable = true, .dense = false },
    void,
).init(allocator);
try b.append(null);
try b.append(.{ .f = 1 });
try b.append(.{ .f = 3 });
try b.append(.{ .i = 5 });
```

You can view [sample.zig](./src/sample.zig) which has an example for all supported types.

### FFI

Arrow has a [C ABI](https://arrow.apache.org/docs/format/CDataInterface.html) that allows in-memory array importing and exporting that only copies metadata.

#### Export

If you have a normal `Array` you can export it to a `abi.Schema` and `abi.Array` to share the memory with other code (i.e. scripting languages). When you do so, that code is responsible for calling `abi.Schema.release(&schema)` and `abi.Array.release(&array)` to free memory.

```zig
var abi_schema = try abi.Schema.init(array);
var abi_arr = try abi.Array.init(array);
try callExternFn(@ptrCast(&abi_schema), @ptrCast(&abi_arr));
```

#### Import

If you have a `abi.Schema` and `abi.Array` you can transform them to an `ImportedArray` that contains a normal `Array`. Be a good steward and free the memory with `imported.deinit()`.

```zig
const array = sample.all();
var abi_schema = try abi.Schema.init(array);
var abi_arr = try abi.Array.init(array);
var imported = try ImportedArray.init(allocator, abi_arr, abi_schema);
defer imported.deinit();
```

### IPC

Array has an [IPC format](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc) to transfer Arrays with zero-copy (unless you add compression or require different alignment). It has a [file format](https://github.com/apache/arrow/blob/main/format/File.fbs) as well.

I cannot in good faith recommend using this format for the following reasons:

1. [Array types](#Usage) are complicated and difficult to generically map to other type systems.
2. Despite claiming to be zero-copy, if an array's buffer uses compression it must be copied. This implementation will also copy is its alignment is not 64 (C++ implementation uses 8).
3. Post-compression size savings compared to CSV are marginal.
4. Poor backwards compatability. There have been 5 versions of the format, most undocumented, with multiple breaking changes.

I also have the following gripes from implementing it:

1. Poor existing tooling. Tools cannot inspect individual messages and have poor error messages. Despite the message format being designed for streaming existing tools work on the entire file at once.
2. Poor documentation. The upstream [`File.fbs`](https://github.com/apache/arrow/blob/main/format/File.fbs) has numerous **incorrect** comments.
3. The message custom metadata that would make the format more useful than just shared `ffi` memory is inaccessible in most implementations (including this one) since they are justifiably focused on record batches.
4. Existing implementations do not support reading/writing record batches with different schemas.

This implementation is only provided as a way to dump normal `Array`s to disk for later inspection.

#### Read

You can read record batches out of an existing Arrow file with `ipc.reader.fileReader`:

```zig
const ipc = @import("arrow").ipc;
var ipc_reader = try ipc.reader.fileReader(allocator, "./testdata/tickers.arrow");
defer ipc_reader.deinit();

while (try ipc_reader.nextBatch()) |rb| {
    // Do something with rb
    defer rb.deinit();
}
```

If feeling daring, you can use the streaming API of `ipc.reader.Reader(ReaderType)`.

#### Write

You can write record batches of a normal `Arrow` array `ipc.writer.fileWriter`:

```zig
const batch = try sample.all(std.testing.allocator);
try batch.toRecordBatch("record batch");
defer batch.deinit();

const fname = "./sample.arrow";
var ipc_writer = try ipc.writer.fileWriter(std.testing.allocator, fname);
defer ipc_writer.deinit();
try ipc_writer.write(batch);
try ipc_writer.finish();
```

If feeling daring, you can use the streaming API of `ipc.writer.Writer(WriterType)`.
