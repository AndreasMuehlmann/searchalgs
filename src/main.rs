use std::{time::{Duration, Instant}, ops::Div};

use rand::prelude::*;
use rayon::prelude::*;

fn generate_random_numbers(length: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    return (0..length).map(|_| rng.gen::<u8>()).collect::<Vec<u8>>()
}

type MultipleSearch = dyn Fn(&Vec<u8>, &Vec<u8>) -> Vec<Option<usize>>;

fn binary_search(searched: u8, arr: &Vec<u8>, mut low: usize, mut high: usize) -> Option<usize> {
   let length = arr.len();
   let mut mid = length / 2;
   let mut current = arr[mid];

   while low <= high { match current.cmp(&searched) { 
            std::cmp::Ordering::Equal => return Some(mid),
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid - 1,
        }
        mid = (high + low) / 2;
        current = arr[mid];
   }
   return None;
}

fn linear_multiple_search(searched_numbers: &Vec<u8>, numbers: &Vec<u8>) -> Vec<Option<usize>> {
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];
    for i in 0..searched_numbers.len() {
        found[i] = binary_search(searched_numbers[i], numbers, 0, numbers.len() - 1);
    }
    found
}

fn multiple_value_search(searched_numbers: &Vec<u8>, numbers: &Vec<u8>) -> Vec<Option<usize>> {
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];
    let mut last_found: usize = 0;
    for i in 0..searched_numbers.len() {
        let option_index = binary_search(searched_numbers[i], numbers, last_found, numbers.len() - 1);
        if let Some(index) = option_index {
            last_found = index;
        }
        found[i] = option_index;
    }
    found
}



fn binary_multiple_search(_searched_numbers: &Vec<u8>, _numbers: &Vec<u8>) -> Vec<Option<usize>> {
    (0..100).map(|num| Some(num)).collect()
}

fn benchmark(multiple_search: &MultipleSearch, length: usize,
             length_searched: usize, iterations: usize, iterations_per_numbers: usize) {
    let mut total_duration: Duration = Duration::from_millis(0);
    for _ in 0..iterations {
        let mut numbers = generate_random_numbers(length);
        numbers.par_sort();
        for _ in 0..iterations_per_numbers {
            let mut searched_numbers = generate_random_numbers(length_searched);
            searched_numbers.par_sort();

            let start = Instant::now();
            let _ = multiple_search(&searched_numbers, &numbers);
            total_duration = total_duration.checked_add(start.elapsed()).unwrap();
        }
    }
    let average_duration = total_duration.div((iterations * iterations_per_numbers) as u32);
    println!("{}", average_duration.as_nanos());
}

fn main() {
    let length = 100000000;
    let iterations = 5;
    benchmark(&linear_multiple_search, length, length / 100, iterations, iterations);
    benchmark(&multiple_value_search, length, length / 100, iterations, iterations);
    benchmark(&binary_multiple_search, length, length / 100, iterations, iterations);
}
