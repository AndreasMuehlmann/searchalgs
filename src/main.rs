use std::io;
use std::time::{Duration, Instant};
use std::ops::Div;
use std::sync::Arc;
use std::error::Error;

use rayon::prelude::*;
use rand::prelude::*;
mod algorithms;
use crate::algorithms::{MultipleSearch, ParallelMultipleSearch};


fn generate_random_numbers(length: usize) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    return (0..length).map(|_| rng.gen::<u64>()).collect::<Vec<u64>>()
}

#[allow(dead_code)]
fn choose_random_numbers(length: usize, vector: &Vec<u64>) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    return (0..length).map(|_| *vector.choose(&mut rng).unwrap()).collect::<Vec<u64>>()
}

#[allow(dead_code)]
fn benchmark(multiple_search: &MultipleSearch, length: usize,
             length_searched: usize, iterations: usize, iterations_per_numbers: usize) {
    let mut total_duration: Duration = Duration::from_millis(0);
    for _ in 0..iterations {
        let mut numbers = generate_random_numbers(length);
        numbers.par_sort();
        for _ in 0..iterations_per_numbers {
            let mut searched_numbers = choose_random_numbers(length_searched, &numbers);
            searched_numbers.par_sort();

            let start = Instant::now();
            let _ = multiple_search(&searched_numbers, &numbers);
            total_duration = total_duration.checked_add(start.elapsed()).unwrap();
        }
    }
    let average_duration = total_duration.div((iterations * iterations_per_numbers) as u32);
    println!("{}", average_duration.as_micros());
}

#[allow(dead_code)]
fn benchmark_par(multiple_search: &ParallelMultipleSearch, length: usize,
             length_searched: usize, iterations: usize, iterations_per_numbers: usize) {
    let mut total_duration: Duration = Duration::from_millis(0);
    for _ in 0..iterations {
        let mut numbers = generate_random_numbers(length);
        numbers.par_sort();
        let numbers = Arc::new(numbers);
        for _ in 0..iterations_per_numbers {
            let mut searched_numbers = choose_random_numbers(length_searched, &numbers);
            //let mut searched_numbers = generate_random_numbers(length_searched);
            searched_numbers.par_sort();
            let searched_numbers = Arc::new(searched_numbers);

            let start = Instant::now();
            let _ = multiple_search(Arc::clone(&searched_numbers), Arc::clone(&numbers));
            total_duration = total_duration.checked_add(start.elapsed()).unwrap();
        }
    }
    let average_duration = total_duration.div((iterations * iterations_per_numbers) as u32);
    println!("{}", average_duration.as_micros());
}

fn vec_equals(vec1: Vec<Option<usize>>, vec2: Vec<Option<usize>>) -> bool {
    if vec2.len() != vec1.len() {
        println!("length not matching");
        println!("vec1 length {}, vec2 length {}", vec1.len(), vec2.len());
        return false
    }
    for index in 0..vec1.len() {
        if vec1[index] != vec2[index] {
            println!("elements at index {} not matching", index);
            if let Some(value) = vec1[index] {
                println!("value vec1: Some {}", value);
            } else {
                println!("value vec1: None");
            }
            if let Some(value) = vec2[index] {
                println!("value vec2: Some {}", value);
            } else {
                println!("value vec2: None");
            }
            return false
        }
    }
    return true
}

#[allow(dead_code)]
fn write_benchmark_data_to_csv() -> Result<(), Box<dyn Error>> {
    let mut wtr = csv::Writer::from_writer(io::stdout());

    wtr.write_record(&["Name", "Place", "ID"])?;

    wtr.serialize(("Mark", "Sydney", 87))?;
    wtr.serialize(("Ashley", "Dublin", 32))?;
    wtr.serialize(("Akshat", "Delhi", 11))?;

    wtr.flush()?;
    Ok(())
}

#[allow(dead_code)]
fn test_algorithm(length: usize, multiple_search: &MultipleSearch) -> bool {
    let mut equal = true;
    for _ in 0..10 {
        let mut numbers = generate_random_numbers(length);
        numbers.par_sort();
        let mut searched_numbers = choose_random_numbers(length / 10, &numbers);
        searched_numbers.par_sort();
        if !vec_equals(algorithms::linear_multiple_search(&searched_numbers, &numbers), multiple_search(&searched_numbers, &numbers)) {
            equal = false;
        } 
    }
    return equal
}

#[allow(dead_code)]
fn test_algorithm_par(length: usize, parallel_multiple_search: &ParallelMultipleSearch) -> bool {
    let mut equal = true;
    for _ in 0..10 {
        let mut numbers = generate_random_numbers(length);
        numbers.par_sort();
        let mut searched_numbers = choose_random_numbers(length / 10, &numbers);
        searched_numbers.par_sort();
        if !vec_equals(
            algorithms::linear_multiple_search(&searched_numbers, &numbers),
            parallel_multiple_search(Arc::new(searched_numbers), Arc::new(numbers))
            ) {
            equal = false;
        } 
    }
    return equal
}

fn main() {
    let length = 10000000;
    let length_searched = length / 5;
    let iterations = 5;

    print!("linear_multiple_search: ");
    benchmark(&algorithms::linear_multiple_search, length, length_searched, iterations, 1);
    print!("multiple_value_search: ");
    benchmark(&algorithms::multiple_value_search, length, length_searched, iterations, 1);
    print!("binary_multiple_search: ");
    benchmark(&algorithms::binary_multiple_search, length, length, iterations, 1);
    print!("split_search: ");
    benchmark(&algorithms::split_search, length, length_searched, iterations, 1);

    print!("parallel_linear_multiple_search: ");
    benchmark_par(&algorithms::parallel_linear_multiple_search, length, length_searched, iterations, 1);
    print!("parallel_rayon_linear_multiple_search: ");
    benchmark_par(&algorithms::parallel_rayon_linear_multiple_search, length, length_searched, iterations, 1);
    print!("parallel_split_search: ");
    benchmark_par(&algorithms::parallel_split_search, length, length_searched, iterations, 1);
    println!();

    /*
    */
}
