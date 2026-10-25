<p align="center">
   <img src="res/rusting_bitmaps_logo_02.png" height="200" />
</p>

# Rusting Bitmaps
Rusting Bitmaps is a native Rust port of [Roaring Bitmaps](https://roaringbitmap.org/) based on CRoaring.

# Differences
Rusting Bitmaps is a loose port of CRoaring with a few differences
 - An idiomatic and safe Rust API
 - Additional optimizations (particularly around inplace operations)
 - Custom allocator support

# Testing
The array set operations have a scalar implementation and an SSE4.2 one that is only compiled in when
the target enables `sse4.2`. A plain `cargo test` exercises the scalar code only, so run the suite a
second time with the feature on:

```
cargo test
RUSTFLAGS="-C target-feature=+sse4.2,+popcnt" CARGO_TARGET_DIR=target/sse cargo test
```

The separate `CARGO_TARGET_DIR` keeps the two builds from rebuilding each other.
On x86_64 Linux the tests in `src/container/array_ops/differential.rs` put the end of every input and output
against an inaccessible page, so a read or write past the end in either implementation crashes the test.

# Future Ideas
 - [ ] See about implementing an inverted array for dense sets
 - [ ] Implement tests
 
