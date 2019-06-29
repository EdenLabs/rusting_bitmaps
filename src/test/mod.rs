#![cfg(test)]

pub mod short;
pub mod extended;

use crate::container::Container;

/// An internal trait for automating test setup
pub trait TestUtils {
    fn create() -> Self;

    fn fill(&mut self, data: &[u16]);
}

/// Create an array container from the given data set
pub fn make_container<T: TestUtils>(data: &[u16]) -> T {
    let mut container = T::create();
    container.fill(data);

    container
}

/// Run a test using the provided executor function and compare it against the expected value
pub fn run_test<T, U, F>(in_a: &[u16], in_b: &[u16], expected: &[u16], f: F) 
    where T: TestUtils,
          U: TestUtils,
          F: Fn(&mut T, &mut U) -> Container 
{
    let mut a = make_container::<T>(in_a);
    let mut b = make_container::<U>(in_b);
    let result = (f)(&mut a, &mut b);

    // Check that the cardinality matches the precomputed result
    let len0 = expected.len();
    let len1 = result.cardinality();
    assert_eq!(
        len0, 
        len1, 
        "\n\nUnequal cardinality; expected {}, found {}.\n\n", 
        len0, 
        len1
    );

    // Check that the output matches the precomputed result
    let pass = result.iter()
        .zip(expected.iter());
    
    let (failed, found, expected) = {
        let mut out_found = 0;
        let mut out_expected = 0;

        let mut failed = false;
        for (found, expected) in pass {
            if found != *expected {
                failed = true;
                out_found = found;
                out_expected = *expected;
                break;
            }
        }

        (failed, out_found, out_expected)
    };

    assert!(!failed, "Sets are not equivalent. Found {}, expected {}", found, expected);
}