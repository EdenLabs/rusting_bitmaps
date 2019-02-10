/// RoaringStatistics can be used to collect detailed statistics about the composition of a roaring bitmap
pub struct RoaringStatistics {
    /// Number of containers
    pub containers: u32,

    /// Number of array containers
    pub array_containers: u32,

    ///  Number of run containers
    pub run_containers: u32,

    /// Number of bitmap containers
    pub bitset_containers: u32,

    /// Number of values in array containers
    pub values_array_containers: u32,

    /// Number of values in run containers
    pub values_run_containers: u32,

    /// Number of values in bitmap containers
    pub values_bitset_containers: u32,

    /// Number of allocated bytes in array containers
    pub bytes_array_containers: u32,

    /// Number of allocated bytes in run containers
    pub bytes_run_containers: u32,

    /// Number of allocated bytes in bitmap containers
    pub bytes_bitset_containers: u32,
    
    /// Maximal value. Undefined if cardinality is zero
    pub max_value: u32,

    /// Minimal value. Undefined if cardinality is zero
    pub min_value: u32,

    /// The sum of all values (could be used to compute average)
    pub sum_value: u64,

    /// Total number of values in the bitmap
    pub cardinality: u64
}

impl RoaringStatistics {
    pub fn new() -> Self {
        Self {
            containers: 0,
            array_containers: 0,
            run_containers: 0,
            bitset_containers: 0,
            values_array_containers: 0,
            values_run_containers: 0,
            values_bitset_containers: 0,
            bytes_array_containers: 0,
            bytes_run_containers: 0,
            bytes_bitset_containers: 0,
            max_value: 0,
            min_value: 0,
            sum_value: 0,
            cardinality:04
        }
    }
}