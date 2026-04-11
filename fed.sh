#!/bin/sh

RUST_LOG=warn cargo run -p optd-cli -- -r fed.sql
