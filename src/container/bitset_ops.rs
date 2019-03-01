pub fn union() {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::union();
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_union();
    }
}

pub fn intersect() {
    
}

pub fn difference() {

}

pub fn symmetric_difference() {
    
}

// Feature set specific implementations

// AVX2 only
#[cfg(target_feature = "avx2")]
mod simd {
    pub fn union() {

    }

    pub fn intersect() {

    }

    pub fn difference() {

    }

    pub fn symmetric_difference() {
        
    }
}

// SSE4.2 only (older feature sets unsupported by the algorithms)
#[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
mod simd {
    pub fn union() {

    }

    pub fn intersect() {

    }

    pub fn difference() {

    }

    pub fn symmetric_difference() {
        
    }
}

// Universal scalar implementations
// 32bit not natively supported but will work albeit slower than an optimized version

fn scalar_union() {

}

fn scalar_intersect() {

}

fn scalar_difference() {

}

fn scalar_symmetric_difference() {
    
}
