# Rusting Bitmaps

><span style="color:red">WARNING: This software is incomplete and unfit for production</span>

Rusting Bitmaps is a native Rust port of Roaring Bitmaps

# Supported Platforms
 - x86_64 with any of the following instruction sets
   - Scalar
   - SSE4.2
   - AVX2

# Feature Implementation Tracking
 - [ ] Roaring Array
    - [ ] TBD
 - [ ] Roaring Bitmap
    - [ ] TBD
 - [x] Traits
 - [-] Array Container
    - [x] Difference
    - [x] Equals
    - [x] Intersect
    - [ ] Negation
    - [x] Subset
    - [x] Union
    - [x] Symmetric Difference
    - [ ] Cardinality variants
    - [ ] Inplace variants
 - [-] Bitset Container
    - [x] Difference
    - [ ] Equals
    - [x] Intersect
    - [ ] Negation
    - [x] Subset
    - [x] Union
    - [x] Symmetric Difference
    - [ ] Cardinality variants
    - [ ] Inplace variants
 - [ ] Run Container
    - [ ] Difference
    - [ ] Equals
    - [ ] Intersect
    - [ ] Negation
    - [ ] Subset
    - [ ] Union
    - [ ] Symmetric Difference
    - [ ] Cardinality variants
    - [ ] Inplace variants
 - [ ] Conversions
    - [ ] Array->Bitmap
    - [ ] Array->Run
    - [ ] Bitmap->Array
    - [ ] Bitmap->Run
    - [ ] Run->Array
    - [ ] Run->Bitmap
 - [ ] Mixed Ops
    - [ ] Difference
    - [ ] Equals
    - [ ] Intersect
    - [ ] Negation
    - [ ] Subset
    - [ ] Union
    - [ ] Symmetric Difference
    - [ ] Cardinality variants
    - [ ] Inplace variants (?)
 - [ ] Serialization (serde w/ feature flag)
 - [x] Portability
 - [ ] COW (?)