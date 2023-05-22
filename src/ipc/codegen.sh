#!/bin/env bash
../../../flatbufferz/zig-out/bin/flatc-zig -o gen -I format format/File.fbs format/Message.fbs format/Schema.fbs
