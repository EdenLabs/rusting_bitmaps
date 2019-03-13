<p align="center">
   <img src="res/rusting_bitmaps_logo_02.png" height="200">
</p>

# Rusting Bitmaps

><span style="color:red">WARNING: This software is incomplete and unfit for production</span>

Rusting Bitmaps is a native Rust port of [Roaring Bitmaps](https://roaringbitmap.org/)

# Supported Platforms
 - x86_64 with optional simd acceleration if built with the following targets
   - SSE4.2
   - AVX2

# Feature Implementation Tracking
 - [ ] Roaring Array
    - [ ] TBD
 - [ ] Roaring Bitmap
    - [ ] TBD
 - [x] Traits
 - [x] Array Container
    - [x] Difference
    - [x] Equals
    - [x] Intersect
    - [x] Negation
    - [x] Subset
    - [x] Union
    - [x] Symmetric Difference
 - [ ] Bitset Container
    - [x] Difference
    - [x] Equals
    - [x] Intersect
    - [x] Negation
    - [x] Subset
    - [x] Union
    - [x] Symmetric Difference
    - [ ] Deferred Cardinality
 - [ ] Run Container
    - [x] Difference
    - [x] Equals
    - [x] Intersect
    - [ ] Negation
    - [x] Subset
    - [x] Union
    - [x] Symmetric Difference
    - [ ] Deferred Cardinality
 - [ ] Conversions
    - [x] Array->Bitmap
    - [x] Array->Run
    - [ ] Bitmap->Array
    - [ ] Bitmap->Run
    - [x] Run->Array
    - [x] Run->Bitmap
 - [ ] Mixed Ops
    - [ ] Difference
    - [ ] Equals
    - [ ] Intersect
    - [ ] Negation
    - [ ] Subset
    - [ ] Union
    - [ ] Symmetric Difference
    - [ ] Cardinality variants?
 - [ ] Serialization (serde w/ feature flag)
 - [x] Portability
 - [ ] Inplace variants for all ops
 - [ ] COW (?)