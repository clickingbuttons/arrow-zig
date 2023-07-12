#!/bin/env bash
script_dir=$(realpath $(dirname "$0"))
rm -rf "$script_dir/gen"
flat_home="$script_dir/../../../flatbuffers-zig"

cd $flat_home
zig build -freference-trace

cd $script_dir
pwd
$flat_home/zig-out/bin/flatc-zig --input-dir format --output-dir gen
