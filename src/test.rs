use std::slice::Iter;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct TestData {
    cases: Vec<TestCase>
}

impl TestData {
    pub fn iter(&self) -> Iter<TestCase> {
        self.cases.iter()
    }
}

#[derive(Serialize, Deserialize)]
pub struct TestCase {
    pub name: &'static str,
    pub a: Vec<u16>,
    pub b: Vec<u16>,
    pub result_or: Vec<u16>,
    pub result_and: Vec<u16>,
    pub result_and_not: Vec<u16>,
    pub result_xor: Vec<u16>
}