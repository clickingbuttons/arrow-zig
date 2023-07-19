const std = @import("std");

pub const name = "arrow";
const path = "src/lib.zig";

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const flatbuffers_dep = b.dependency("flatbuffers-zig", .{
        .target = target,
        .optimize = optimize,
    });
    const flatbuffers_mod = flatbuffers_dep.module("flatbuffers");

    const lz4 = b.dependency("lz4", .{
        .target = target,
        .optimize = optimize,
    });
    const lz4_mod = lz4.module("lz4");
    // Expose to zig dependents
    const module = b.addModule(name, .{
        .source_file = .{ .path = path },
        .dependencies = &.{
            .{ .name = "flatbuffers", .module = flatbuffers_mod },
            .{ .name = "lz4", .module = lz4_mod },
        },
    });

    const lib = b.addSharedLibrary(.{
        .name = "arrow-zig", // Avoid naming conflict with libarrow
        .root_source_file = .{ .path = path },
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(lib);

    const test_step = b.step("test", "Run library tests");
    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = path },
        .target = target,
        .optimize = optimize,
    });
    main_tests.addModule("lz4", lz4_mod);
    main_tests.addModule("flatbuffers", flatbuffers_mod);
    const run_main_tests = b.addRunArtifact(main_tests);
    test_step.dependOn(&run_main_tests.step);

    const integration_test_step = b.step(
        "test-integration",
        "Run integration tests (requires Python 3 + pyarrow >= 10.0.0)",
    );
    const ffi_test = b.addSystemCommand(&[_][]const u8{ "python", "test_ffi.py" });
    ffi_test.step.dependOn(&run_main_tests.step);
    ffi_test.step.dependOn(b.getInstallStep());
    const ipc_test = b.addSystemCommand(&[_][]const u8{ "python", "test_ipc.py" });
    ipc_test.step.dependOn(&run_main_tests.step);
    integration_test_step.dependOn(&ipc_test.step);
    integration_test_step.dependOn(&ffi_test.step);

    const example_test_step = b.step("test-examples", "Run example tests");
    const example_tests = b.addTest(.{
        .root_source_file = .{ .path = "./examples/all.zig" },
        .target = target,
        .optimize = optimize,
    });
    example_tests.addModule("arrow", module);
    const run_example_tests = b.addRunArtifact(example_tests);
    example_test_step.dependOn(&run_example_tests.step);
}
