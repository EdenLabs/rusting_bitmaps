pub mod short;
pub mod extended;

/// An internal trait for automating test setup
trait TestUtils {
    fn create() -> Self;

    fn fill(&mut self, data: &[u16]);
}

impl TestUtils for ArrayContainer {
    fn create() -> Self {
        Self::new()
    }

    fn fill(&mut self, data: &[u16]) {
        for value in data.iter() {
            self.add(*value);
        }
    }
}

impl TestUtils for BitsetContianer {
    fn create() -> Self {
        Self::new()
    }

    fn fill(&mut self, data: &[u16]) {
        self.set_list(data);
    }
}

impl TestUtils for RunContainer {
    fn create() -> Self {
        Self::new()
    }

    fn fill(&mut self, data: &[u16]) {
        for value in data.iter() {
            self.add(*value);
        }
    }
}

/// Create an array container from the given data set
fn make_container<T: TestUtils>(data: &[u16]) -> T<u16> {
    let mut container = T::create();
    container.fill(data);

    container
}

/// Run a test using the provided executor function and compare it against the expected value
fn run_test<T, U, F>(data: &[u16], f: F) 
    where T: TestUtils,
          U: TestUtils,
          F: Fn(&mut T, &mut U) -> Container 
{
    let mut a = make_container::<T>(&DATA_A);
    let mut b = make_container::<U>(&DATA_B);
    let result = (f)(&mut a, &mut b);

    // Check that the cardinality matches the precomputed result
    let len0 = result.len();
    let len1 = data.len();
    assert_eq!(
        len0, 
        len1, 
        "\n\nUnequal cardinality; expected {}, found {}.\n\n", 
        len0, 
        len1
    );

    // Check that the output matches the precomputed result
    let pass = result.iter()
        .zip(data.iter());
    
    let (failed, found, expected) = {
        let mut out_found = 0;
        let mut out_expected = 0;

        let mut failed = false;
        for (found, expected) in pass {
            if found != expected {
                failed = true;
                out_found = *found;
                out_expected = *expected;
                break;
            }
        }

        (failed, out_found, out_expected)
    };

    assert!(!failed, "Sets are not equivalent. Found {}, expected {}", found, expected);
}