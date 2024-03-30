use std::time::{Duration, Instant};
use std::collections::VecDeque;
use std::ops::Div;
use std::thread;
use std::sync::Arc;

use rand::prelude::*;
use rayon::prelude::*;


#[allow(dead_code)]
fn generate_random_numbers(length: usize) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    return (0..length).map(|_| rng.gen::<u64>()).collect::<Vec<u64>>()
}

#[allow(dead_code)]
fn choose_random_numbers(length: usize, vector: &Vec<u64>) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    return (0..length).map(|_| *vector.choose(&mut rng).unwrap()).collect::<Vec<u64>>()
}

type MultipleSearch = dyn Fn(&Vec<u64>, &Vec<u64>) -> Vec<Option<usize>>;
type ParallelMultipleSearch = dyn Fn(Arc<Vec<u64>>, Arc<Vec<u64>>) -> Vec<Option<usize>>;

#[allow(dead_code)]
fn linear_multiple_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];
    for i in 0..searched_numbers.len() {
        found[i] = match numbers.binary_search(&searched_numbers[i]) {
            Ok(index) => Some(index),
            Err(_) => None,
        };
    }
    found
}

// HAS BUG
#[allow(dead_code)]
fn multiple_value_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];
    let mut last_found: usize = 0;
    for i in 0..searched_numbers.len() {
        let result = &numbers[last_found..].binary_search(&searched_numbers[i]);
        found[i] = match result {
            Ok(index) => {
                last_found += *index;
                Some(last_found)
            },
            Err(index) => {
                last_found += *index;
                last_found = last_found.saturating_sub(1);
                None
            },
        };
    }
    found
}

#[allow(dead_code)]
#[derive(Debug)]
struct SearchTask {
    low: usize,
    high: usize,
    low_searched: usize,
    high_searched: usize,
}

#[inline(always)]
fn mid(low: usize, high: usize) -> usize {
    low + (high - low) / 2
}

// HAS BUG
#[allow(dead_code)]
fn binary_multiple_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];
    let mut stack: VecDeque<SearchTask> = VecDeque::with_capacity(
        (searched_numbers.len() as f64).log2() as usize + 100
        );
    stack.push_back(SearchTask { 
        // high and low are included
        low: 0,
        high: numbers.len() - 1,
        low_searched: 0,
        high_searched: searched_numbers.len() - 1,
    });
    loop {
        let c_option = stack.pop_back();
        match c_option {
            Some(c) => {
                if c.low > c.high || c.low_searched > c.high_searched {
                    continue
                }
                let searched_index: usize = mid(c.low_searched, c.high_searched);
                let result = numbers[c.low..c.high + 1].binary_search(&searched_numbers[searched_index]);
                match result {
                    Ok(index) => {
                        let index = index + c.low;
                        found[searched_index] = Some(index);
                        if c.low_searched == c.high_searched {
                            continue;
                        }
                        stack.push_back(SearchTask { 
                            low: c.low,
                            high: index - 1,
                            low_searched: c.low_searched,
                            high_searched: searched_index.saturating_sub(1),
                        });
                        stack.push_back(SearchTask { 
                            low: index + 1,
                            high: c.high,
                            low_searched: searched_index + 1,
                            high_searched: c.high_searched,
                        });
                    },
                    Err(index) => {
                        let index = index + c.low;
                        if c.low_searched == c.high_searched {
                            continue;
                        }
                        stack.push_back(SearchTask { 
                            low: c.low,
                            high: index,
                            low_searched: c.low_searched,
                            high_searched: searched_index.saturating_sub(1),
                        });
                        stack.push_back(SearchTask { 
                            low: index,
                            high: c.high,
                            low_searched: searched_index + 1,
                            high_searched: c.high_searched,
                        });
                    },
                }
            },
            None => break,
        }
    }
    found
}

#[allow(dead_code)]
fn get_split_indices(length: usize, split_index_skip: usize) -> Vec<(usize, usize)> {
    let mut split_indices: Vec<(usize, usize)> = Vec::new();
    for i in 1..(length/split_index_skip + 1).max(2) {
        split_indices.push((
                (i - 1) * split_index_skip,
                (i * split_index_skip - 1).min(length - 1),
                ));
    }
    split_indices
}

#[allow(dead_code)]
fn split_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    const SPLIT_INDEX_SKIP: usize = 100;
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];

    for (low_searched, high_searched) in get_split_indices(searched_numbers.len(), SPLIT_INDEX_SKIP) {
        let low_found: usize = match numbers.binary_search(&searched_numbers[low_searched]) {
            Ok(index) => {
                found[low_searched] = Some(index);
                index
            },
            Err(index) => index,
        };
        let high_found: usize = match numbers.binary_search(&searched_numbers[high_searched]) {
            Ok(index) => {
                found[high_searched] = Some(index);
                index + 1
            },
            Err(index) => index,
        };

        for index in low_searched + 1..high_searched {
            found[index] = match numbers[low_found..high_found].binary_search(&searched_numbers[index]) {
                Ok(index) => Some(low_found + index),
                Err(_) => None,
            };
        }
    }
    found
}

#[allow(dead_code)]
fn parallel_linear_multiple_search(searched_numbers: Arc<Vec<u64>>, numbers: Arc<Vec<u64>>) -> Vec<Option<usize>> {
    const THREADS: usize = 16;
    if searched_numbers.len() < THREADS * 2 {
        println!("searcherd_numbers not long enough");
        return linear_multiple_search(&searched_numbers, &numbers)
    }
    let searched_numbers = Arc::new(searched_numbers);
    let numbers = Arc::new(numbers);
    let mut handles = Vec::new();
    for i in 0..THREADS {
        let searched_numbers = Arc::clone(&searched_numbers);
        let numbers = Arc::clone(&numbers);
        let low = i * searched_numbers.len() / THREADS;
        let high = ((i + 1) * searched_numbers.len() / THREADS).min(searched_numbers.len());

        let handle = thread::spawn(move || {
            let mut found: Vec<Option<usize>> = vec![None; high - low];
            for i in low..high {
                found[i - low] = match numbers.binary_search(&searched_numbers[i]) {
                    Ok(index) => Some(index),
                    Err(_) => None,
                };
            }
            found
        });

        handles.push(handle);
    }
    let mut found = Vec::with_capacity(searched_numbers.len());
    for handle in handles {
        found.extend(handle.join().unwrap().iter())
    }

    found
}

#[allow(dead_code)]
fn parallel_split_search(searched_numbers: Arc<Vec<u64>>, numbers: Arc<Vec<u64>>) -> Vec<Option<usize>> {
    const THREADS: usize = 15;
    const SPLIT_INDEX_SKIP: usize = 1000;
    if searched_numbers.len() < SPLIT_INDEX_SKIP * 2 {
        println!("searcherd_numbers not long enough");
        return linear_multiple_search(&searched_numbers, &numbers)
    }
    let split_indices: Vec<(usize, usize)> = get_split_indices(searched_numbers.len(), SPLIT_INDEX_SKIP);
    let mut handles = Vec::new();
    for i in 0..THREADS {
        let searched_numbers = Arc::clone(&searched_numbers);
        let numbers = Arc::clone(&numbers);
        let thread_work = split_indices.len() / THREADS + 1;
        let split_indices_thread: Vec<(usize, usize)> = split_indices.clone().into_iter().skip(i * thread_work).take(thread_work).collect();
        let handle = thread::spawn(move || {
            if split_indices_thread.is_empty() { return Vec::new() }
            let low_thread = split_indices_thread[0].0;
            let high_thread = split_indices_thread[split_indices_thread.len() - 1].1;
            let mut found_thread: Vec<Option<usize>> = vec![None; high_thread - low_thread + 1];
            for (low, high) in split_indices_thread {
                let low_found = match numbers.binary_search(&searched_numbers[low]) {
                    Ok(index) => {
                        found_thread[low - low_thread] = Some(index);
                        index
                    },
                    Err(index) => index,
                };
                let high_found = match numbers.binary_search(&searched_numbers[high]) {
                    Ok(index) => {
                        found_thread[high - low_thread] = Some(index);
                        index + 1
                    },
                    Err(index) => index,
                };
                for i in low + 1..high {
                    found_thread[i - low] = match numbers[low_found..high_found].binary_search(&searched_numbers[i]) {
                        Ok(index) => Some(low_found + index),
                        Err(_) => None,
                    };
                }
            }
            found_thread
        });

        handles.push(handle);
    }
    let mut found = Vec::with_capacity(searched_numbers.len());
    for handle in handles {
        found.extend(handle.join().unwrap().iter())
    }

    found
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

#[allow(dead_code)]
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

fn main() {
    let length = 1000000;
    let length_searched = length / 10;
    let iterations = 10;
    benchmark(&linear_multiple_search, length, length_searched, iterations, 1);
    benchmark(&multiple_value_search, length, length_searched, iterations, 1);
    //benchmark(&binary_multiple_search, length, length / 10000, iterations, iterations);
    benchmark(&split_search, length, length_searched, iterations, 1);

    benchmark_par(&parallel_linear_multiple_search, length, length_searched, iterations, 1);
    benchmark_par(&parallel_split_search, length, length_searched, iterations, 1);
    /*
    for i in 0..100 {
        let mut numbers = generate_random_numbers(length);
        numbers.par_sort();
        let mut searched_numbers = choose_random_numbers(length / 10, &numbers);
        searched_numbers.par_sort();
        if vec_equals(linear_multiple_search(&searched_numbers, &numbers), multiple_value_search(&searched_numbers, &numbers)) {
            //println!("equal");
        } else {
            println!("not equal")
        }
    }
    */
}
