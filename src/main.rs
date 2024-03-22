use std::time::{Duration, Instant};
use std::collections::VecDeque;
use std::ops::Div;
use std::thread;
use std::sync::{Arc, mpsc};

use rand::prelude::*;
use rayon::iter::IndexedParallelIterator;
use rayon::prelude::*;


fn generate_random_numbers(length: usize) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    return (0..length).map(|_| rng.gen::<u64>()).collect::<Vec<u64>>()
}

fn choose_random_numbers(length: usize, vector: &Vec<u64>) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    return (0..length).map(|_| *vector.choose(&mut rng).unwrap()).collect::<Vec<u64>>()
}

type MultipleSearch = dyn Fn(&Vec<u64>, &Vec<u64>) -> Vec<Option<usize>>;
type ParallelMultipleSearch = dyn Fn(Vec<u64>, Vec<u64>) -> Vec<Option<usize>>;

fn _binary_search(searched: u64, arr: &Vec<u64>, mut low: usize, mut high: usize) -> Option<usize> {
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

fn multiple_value_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];
    let mut last_found: usize = 0;
    for i in 0..searched_numbers.len() {
        let result = &numbers[last_found..].binary_search(&searched_numbers[i]);
        found[i] = match result {
            Ok(index) => {
                last_found += *index; 
                Some(*index)
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
    //let mut count = 0;
    loop {
        /*if count >= 4 {
            break;
        }*/
        let c_option = stack.pop_back();
        match c_option {
            Some(c) => {
                //let c = dbg!(c);
                if c.low > c.high || c.low_searched > c.high_searched {
                    //println!("ignored");
                    continue
                }
                let searched_index: usize = mid(c.low_searched, c.high_searched);
                //let searched_index = dbg!(searched_index);
                let result = numbers[c.low..c.high + 1].binary_search(&searched_numbers[searched_index]);
                match result {
                    Ok(index) => {
                        let index = index + c.low;
                        //println!("found at {}", index);
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
                        //println!("inserted at {}", index);
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
        //count += 1;
    }
    found
}

fn split_search(searched_numbers: &Vec<u64>, numbers: &Vec<u64>) -> Vec<Option<usize>> {
    let split_index_skip: usize = 100;
    let mut found: Vec<Option<usize>> = vec![None; searched_numbers.len()];

    let mut low_searched: usize;
    let mut high_searched: usize;
    let mut low_found: usize;
    let mut high_found: usize;
    for i in 1..(searched_numbers.len()/split_index_skip + 1).max(2) {
        low_searched = (i - 1) * split_index_skip;
        high_searched = (i * split_index_skip).min(searched_numbers.len() - 1);

        low_found = match numbers.binary_search(&searched_numbers[low_searched]) {
            Ok(index) => {
                found[low_searched] = Some(index);
                index
            },
            Err(index) => index,
        };
        high_found = match numbers.binary_search(&searched_numbers[high_searched]) {
            Ok(index) => {
                found[high_searched] = Some(index);
                index
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

fn parallel_linear_multiple_search(searched_numbers: Vec<u64>, numbers: Vec<u64>) -> Vec<Option<usize>> {
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

fn parallel_partial_split_search(searched_numbers: Vec<u64>, numbers: Vec<u64>) -> Vec<Option<usize>> {
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

            let low_found = match numbers.binary_search(&searched_numbers[low]) {
                Ok(index) => {
                    found[0] = Some(index);
                    index
                },
                Err(index) => index,
            };
            let high_found = match numbers.binary_search(&searched_numbers[high - 1]) {
                Ok(index) => {
                    let last = found.len() - 1;
                    found[last] = Some(index);
                    index
                },
                Err(index) => index,
            };
            for i in low + 1..high - 1 {
                found[i - low] = match numbers[low_found..high_found].binary_search(&searched_numbers[i]) {
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
    println!("{}", average_duration.as_nanos());
}

fn benchmark_par(multiple_search: &ParallelMultipleSearch, length: usize,
             length_searched: usize, iterations: usize) {
    let mut total_duration: Duration = Duration::from_millis(0);
    for _ in 0..iterations {
        let mut numbers = generate_random_numbers(length);
        numbers.par_sort();
        let mut searched_numbers = choose_random_numbers(length_searched, &numbers);
        searched_numbers.par_sort();

        let start = Instant::now();
        let _ = multiple_search(searched_numbers, numbers);
        total_duration = total_duration.checked_add(start.elapsed()).unwrap();
    }
    let average_duration = total_duration.div(iterations as u32);
    println!("{}", average_duration.as_nanos());

}

fn main() {
    let length = 500000000;
    let iterations = 1;
    benchmark(&linear_multiple_search, length, length / 10, iterations, iterations);
    //benchmark(&multiple_value_search, length, length / 10000, iterations, iterations);
    //benchmark(&binary_multiple_search, length, length / 10000, iterations, iterations);
    benchmark(&split_search, length, length / 10, iterations, iterations);
    benchmark_par(&parallel_linear_multiple_search, length, length / 100, 1);
    benchmark_par(&parallel_partial_split_search, length, length / 100, 1);
    /*
    let mut numbers = generate_random_numbers(length);
    numbers.par_sort();
    let mut searched_numbers = choose_random_numbers(length / 5, &numbers);
    searched_numbers.par_sort();
    dbg!(split_search(&searched_numbers, &numbers));
    dbg!(linear_multiple_search(&searched_numbers, &numbers));
    */
}
