const std = @import("std");

pub const name = "arrow";
const path = "src/lib.zig";

pub fn build(b: *std.Build) !void {
    // Expose to zig dependents
    _ = b.addModule(name, .{ .source_file = .{ .path = path } });

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addSharedLibrary(.{
        .name = name,
        .root_source_file = .{ .path = path },
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(lib);

    const flatbuffers_dep = b.dependency("flatbuffers-zig", .{
        .target = target,
        .optimize = optimize,
    });
    const flatbuffers_mod = flatbuffers_dep.module("flatbuffers");
    lib.addModule("flatbuffers", flatbuffers_mod); // For generated files to use lib

    const lz4 = b.dependency("lz4", .{
        .target = target,
        .optimize = optimize,
    });
    const lz4_mod = lz4.module("lz4");
    lib.addModule("lz4", lz4_mod);

    const test_step = b.step("test", "Run library tests");
    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = path },
        .target = target,
        .optimize = optimize,
    });
    main_tests.addModule("flatbuffers", flatbuffers_mod); // For generated files to use lib
    main_tests.addModule("lz4", lz4_mod);
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
}
