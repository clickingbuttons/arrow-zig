const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addSharedLibrary(.{
        .name = "arrow-zig",
        .root_source_file = .{ .path = "src/lib.zig" },
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

    const gen_step = b.step("gen", "Run flatc-zig for codegen");
    lib.step.dependOn(gen_step);
    const ipc_gen_dir = try std.fs.path.join(b.allocator, &[_][]const u8{
        "src",
        "ipc",
        "gen",
    });
    defer b.allocator.free(ipc_gen_dir);
    const ipc_fbs_dir = try std.fs.path.join(b.allocator, &[_][]const u8{
        "src",
        "ipc",
        "format",
    });
    defer b.allocator.free(ipc_fbs_dir);
    const flatc = flatbuffers_dep.artifact("flatc-zig");
    const run_flatc = b.addRunArtifact(flatc);
    run_flatc.addArgs(&[_][]const u8{
        "--input-dir",
        ipc_fbs_dir,
        "--output-dir",
        ipc_gen_dir,
    });

    const lz4 = b.dependency("lz4", .{
        .target = target,
        .optimize = optimize,
    });
    const lz4_mod = lz4.module("lz4");
    lib.addModule("lz4", lz4_mod);

    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/lib.zig" },
        .target = target,
        .optimize = optimize,
    });
    main_tests.addModule("flatbuffers", flatbuffers_mod); // For generated files to use lib
    main_tests.addModule("lz4", lz4_mod);
    const run_main_tests = b.addRunArtifact(main_tests);
    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_main_tests.step);
}
