use std::collections::VecDeque;
use std::thread;
use std::sync::Arc;

use rayon::prelude::*;

pub type MultipleSearch = dyn Fn(&Vec<u64>, &Vec<u64>) -> Vec<Option<usize>>;
pub type ParallelMultipleSearch = dyn Fn(Arc<Vec<u64>>, Arc<Vec<u64>>) -> Vec<Option<usize>>;

#[allow(dead_code)]
pub fn linear_multiple_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];
    for i in 0..searched_numbers.len() {
        found[i] = match numbers.binary_search(&searched_numbers[i]) {
            Ok(index) => Some(index),
            Err(_) => None,
        };
    }
    found
}

#[allow(dead_code)]
pub fn multiple_value_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
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
                None
            },
        };
    }
    found
}

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

pub fn binary_multiple_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    const COERSION_LENGTH: usize = 100;
    if searched_numbers.len() < COERSION_LENGTH * 2 {
        println!("searcherd_numbers not long enough");
        return linear_multiple_search(&searched_numbers, &numbers)
    }

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
            None => break,
            Some(c) => {
                if c.high_searched - c.low_searched <= 1000 {
                    for index in c.low_searched..c.high_searched + 1 {
                        found[index] = match numbers[c.low..c.high + 1].binary_search(&searched_numbers[index]) {
                            Ok(index) => Some(c.low + index),
                            Err(_) => None,
                        };
                    }
                    continue
                }

                let searched_index: usize = mid(c.low_searched, c.high_searched);
                let result = numbers[c.low..c.high + 1].binary_search(&searched_numbers[searched_index]);
                match result {
                    Ok(index) => {
                        let index = index + c.low;
                        found[searched_index] = Some(index);
                        stack.push_back(SearchTask { 
                            low: c.low,
                            high: index,
                            low_searched: c.low_searched,
                            high_searched: searched_index - 1,
                        });
                        stack.push_back(SearchTask { 
                            low: index,
                            high: c.high,
                            low_searched: searched_index + 1,
                            high_searched: c.high_searched,
                        });
                    },
                    Err(index) => {
                        let index = index + c.low;
                        stack.push_back(SearchTask { 
                            low: c.low,
                            high: index,
                            low_searched: c.low_searched,
                            high_searched: searched_index - 1,
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
        }
    }
    found
}

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
pub fn split_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    const SPLIT_INDEX_SKIP: usize = 100;
    let mut found: Vec<Option<usize>> = Vec::with_capacity(searched_numbers.len());
    for (low_searched, high_searched) in get_split_indices(searched_numbers.len(), SPLIT_INDEX_SKIP) {
        let mut low_found = 0;
        let high_found = match numbers.binary_search(&searched_numbers[high_searched]) {
            Ok(index) => index + 1,
            Err(index) => index,
        };
        for searched_number in searched_numbers[low_searched..high_searched + 1].iter() {
            found.push(match numbers[low_found..high_found].binary_search(searched_number) {
                Ok(index) => {
                    low_found += index;
                    Some(low_found)
                },
                Err(index) => {
                    low_found += index;
                    None
                },
            });
        }
    }
    found
}

#[allow(dead_code)]
pub fn parallel_linear_multiple_search(searched_numbers: Arc<Vec<u64>>, numbers: Arc<Vec<u64>>) -> Vec<Option<usize>> {
    const THREADS: usize = 16;
    if searched_numbers.len() < THREADS * 2 {
        println!("searcherd_numbers not long enough");
        return linear_multiple_search(&searched_numbers, &numbers)
    }
    let searched_numbers = Arc::new(searched_numbers);
    let numbers = Arc::new(numbers);
    let mut handles = Vec::with_capacity(THREADS);
    for i in 0..THREADS {
        let searched_numbers = Arc::clone(&searched_numbers);
        let numbers = Arc::clone(&numbers);

        let low = i * searched_numbers.len() / THREADS;
        let high = ((i + 1) * searched_numbers.len() / THREADS).min(searched_numbers.len());

        let handle = thread::spawn(move || {
            let mut found: Vec<Option<usize>> = Vec::with_capacity(high - low);
            for i in low..high {
                found.push(match numbers.binary_search(&searched_numbers[i]) {
                    Ok(index) => Some(index),
                    Err(_) => None,
                });
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
pub fn parallel_rayon_linear_multiple_search(searched_numbers: Arc<Vec<u64>>, numbers: Arc<Vec<u64>>) -> Vec<Option<usize>> {
    searched_numbers.par_iter()
        .map(|searched_number| match numbers.binary_search(&searched_number) {
                    Ok(index) => Some(index),
                    Err(_) => None,
                })
        .collect()
}

#[allow(dead_code)]
pub fn parallel_split_search(searched_numbers: Arc<Vec<u64>>, numbers: Arc<Vec<u64>>) -> Vec<Option<usize>> {
    const THREADS: usize = 15;
    const SPLIT_INDEX_SKIP: usize = 1000;
    if searched_numbers.len() < SPLIT_INDEX_SKIP * 2 {
        println!("searcherd_numbers not long enough");
        return linear_multiple_search(&searched_numbers, &numbers)
    }
    let split_indices: Vec<(usize, usize)> = get_split_indices(searched_numbers.len(), SPLIT_INDEX_SKIP);
    let mut handles = Vec::with_capacity(THREADS);
    for i in 0..THREADS {
        let searched_numbers = Arc::clone(&searched_numbers);
        let numbers = Arc::clone(&numbers);

        let thread_work = split_indices.len().div_ceil(THREADS);
        let split_indices_thread: Vec<(usize, usize)> = split_indices[(i * thread_work).min(split_indices.len())..((i + 1) * thread_work).min(split_indices.len())].to_vec();

        let handle = thread::spawn(move || {
            if split_indices_thread.is_empty() { return Vec::new() }

            let low_thread = split_indices_thread[0].0;
            let high_thread = split_indices_thread[split_indices_thread.len() - 1].1;
            let low_found_thread = unsafe { 
                numbers.binary_search(&searched_numbers[low_thread]).unwrap_unchecked()
            };
            let mut found_thread: Vec<Option<usize>> = Vec::with_capacity(high_thread - low_thread + 1);
            for (low, high) in split_indices_thread {
                let mut low_found = low_found_thread;
                let high_found = match numbers.binary_search(&searched_numbers[high]) {
                    Ok(index) => index + 1,
                    Err(index) => index,
                };
                for searched_number in searched_numbers[low..high + 1].iter() {
                    found_thread.push(match numbers[low_found..high_found].binary_search(searched_number) {
                        Ok(index) => {
                            low_found += index;
                            Some(low_found)
                        },
                        Err(index) => {
                            low_found += index;
                            None
                        },
                    });
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
