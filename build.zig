const std = @import("std");

pub fn build(b: *std.Build) void {
	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	const lib = b.addSharedLibrary(.{
		.name = "arrow-zig",
		.root_source_file = .{ .path = "src/lib.zig" },
		.target = target,
		.optimize = optimize,
	});
	b.installArtifact(lib);

	const flatbufferz_dep = b.dependency("flatbufferz", .{
		.target = target,
		.optimize = optimize,
	});
	const flatbufferz_mod = flatbufferz_dep.module("flatbufferz");
	lib.addModule("flatbufferz", flatbufferz_mod); // For generated files to use lib

	// TODO: figure out how to run flatc as part of build process, but only when asked or dep is
	// updated. see ./flatbuffers.sh
	// const flatc = flatbufferz_dep.artifact("flatc-zig");
	// b.installArtifact(flatc);
	// const flatc_run = b.addRunArtifact(flatc);
	// flatc_run.has_side_effects = true;
	// flatc_run.step.dependOn(b.getInstallStep());
	// if (b.args) |args| flatc_run.addArgs(args);

	const lz4 = b.dependency("lz4", .{
		.target = target,
		.optimize = optimize,
	});
	const lz4_mod = lz4.module("lz4");
	lib.linkLibrary(lz4.artifact("lz4"));
	lib.addModule("lz4", lz4_mod);

	const main_tests = b.addTest(.{
		.root_source_file = .{ .path = "src/lib.zig" },
		.target = target,
		.optimize = optimize,
	});
	main_tests.addModule("flatbufferz", flatbufferz_mod); // For generated files to use lib
	main_tests.addModule("lz4", lz4_mod);
	const run_main_tests = b.addRunArtifact(main_tests);
	const test_step = b.step("test", "Run library tests");
	test_step.dependOn(&run_main_tests.step);
}
