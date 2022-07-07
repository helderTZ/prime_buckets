use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use rayon::prelude::*;
use clap::Parser;

/// Computes prime numbers and counts how many primes
/// end with the digits '1', '3', '7' and '9'
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Upper bound, compute primes up to this number
    #[clap(short, long, value_parser)]
    number: u64,

    /// Enable parallelization
    #[clap(short, long, value_parser, default_value_t = false)]
    par: bool,

    /// Dump primes calculated (dumps each bucket separately)
    #[clap(short, long, value_parser, default_value_t = false)]
    dump: bool,
}

/// Checks if a number is prime
fn is_prime(number: u64) -> bool {
    let mut found_factor = false;
    for i in 2..((number as f64).sqrt() as u64) {
        if number % i == 0 {
            found_factor = true;
            continue;
        }
        if found_factor {
            return false;
        }
   }

   if found_factor {
        return false;
    }

   true
}

/// Returns last digit of a number
fn last_digit(number: u64) -> u64 {
    number % 10
}

/// Calculates primes up to 'number' and divides
/// them among buckets depending on their last digit
fn prime_buckets(number: u64) -> (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>) {
    let mut bucket_end1 : Vec<u64> = vec![];
    let mut bucket_end3 : Vec<u64> = vec![];
    let mut bucket_end7 : Vec<u64> = vec![];
    let mut bucket_end9 : Vec<u64> = vec![];

    for i in 1..number {
        if is_prime(i) {
            match last_digit(i) {
                1 => bucket_end1.push(i),
                3 => bucket_end3.push(i),
                7 => bucket_end7.push(i),
                9 => bucket_end9.push(i),
                _ => {}
            }
        }
    }

    (bucket_end1, bucket_end3, bucket_end7, bucket_end9)
}

/// Same as 'prime_buckets' but it's a parallel implementation
fn prime_buckets_par(number: u64) -> (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>) {
    let bucket_end1 : Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(vec![]));
    let bucket_end3 : Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(vec![]));
    let bucket_end7 : Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(vec![]));
    let bucket_end9 : Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(vec![]));

    (1..number).into_par_iter().for_each(|i| {
        if is_prime(i) {
            match last_digit(i) {
                1 => bucket_end1.lock().unwrap().push(i),
                3 => bucket_end3.lock().unwrap().push(i),
                7 => bucket_end7.lock().unwrap().push(i),
                9 => bucket_end9.lock().unwrap().push(i),
                _ => {}
            }
        }
    });

    (Arc::try_unwrap(bucket_end1).unwrap().into_inner().unwrap(),
     Arc::try_unwrap(bucket_end3).unwrap().into_inner().unwrap(),
     Arc::try_unwrap(bucket_end7).unwrap().into_inner().unwrap(),
     Arc::try_unwrap(bucket_end9).unwrap().into_inner().unwrap())
}

fn main() {
    let args = Args::parse();

    let duration;
    let mut bucket_end1 : Vec<u64>;
    let mut bucket_end3 : Vec<u64>;
    let mut bucket_end7 : Vec<u64>;
    let mut bucket_end9 : Vec<u64>;

    if args.par {
        println!("Running in parallel");
        let start = Instant::now();
        (bucket_end1, bucket_end3, bucket_end7, bucket_end9) = prime_buckets_par(args.number);
        duration = start.elapsed();
    }
    else {
        println!("Running in serial");
        let start = Instant::now();
        (bucket_end1, bucket_end3, bucket_end7, bucket_end9) = prime_buckets(args.number);
        duration = start.elapsed();
    }

    println!("Took {}s", duration.as_secs_f64());
    println!("Total primes ending in 1: {}", bucket_end1.len());
    println!("Total primes ending in 3: {}", bucket_end3.len());
    println!("Total primes ending in 7: {}", bucket_end7.len());
    println!("Total primes ending in 9: {}", bucket_end9.len());

    if args.dump {
        bucket_end1.sort();
        bucket_end3.sort();
        bucket_end7.sort();
        bucket_end9.sort();
        let strings1 : Vec<String> = bucket_end1.iter().map(|n| n.to_string()).collect();
        let strings3 : Vec<String> = bucket_end3.iter().map(|n| n.to_string()).collect();
        let strings7 : Vec<String> = bucket_end7.iter().map(|n| n.to_string()).collect();
        let strings9 : Vec<String> = bucket_end9.iter().map(|n| n.to_string()).collect();
        let mut f1 = File::create("bucket1.txt").expect("Could not create file");
        let mut f3 = File::create("bucket3.txt").expect("Could not create file");
        let mut f7 = File::create("bucket7.txt").expect("Could not create file");
        let mut f9 = File::create("bucket9.txt").expect("Could not create file");
        write!(f1, "{}", strings1.join(", ")).expect("Could not write to file");
        write!(f3, "{}", strings3.join(", ")).expect("Could not write to file");
        write!(f7, "{}", strings7.join(", ")).expect("Could not write to file");
        write!(f9, "{}", strings9.join(", ")).expect("Could not write to file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_digit_test() {
        assert_eq!(last_digit(29), 9);
        assert_eq!(last_digit(654676), 6);
        assert_eq!(last_digit(20), 0);
        assert_eq!(last_digit(3), 3);
    }

    #[test]
    fn is_prime_test() {
        assert_eq!(is_prime(13), true);
        assert_eq!(is_prime(10), false);
        assert_eq!(is_prime(222), false);
        assert_eq!(is_prime(1), true);
        assert_eq!(is_prime(3), true);
        assert_eq!(is_prime(5), true);
        assert_eq!(is_prime(7), true);
        assert_eq!(is_prime(9), true);
    }

    #[test]
    fn prime_buckets_test() {
        let number = 10;
        let (bucket1, bucket3, bucket7, bucket9) = prime_buckets(number);
        assert_eq!(bucket1, vec![1]);
        assert_eq!(bucket3, vec![3]);
        assert_eq!(bucket7, vec![7]);
        assert_eq!(bucket9, vec![9]);
    }
}