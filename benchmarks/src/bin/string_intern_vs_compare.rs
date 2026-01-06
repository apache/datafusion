use datafusion_common::instant::Instant;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::env;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

fn gen_random_string(rng: &mut StdRng, len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let idx = rng.gen_range(0..CHARS.len());
        let c = CHARS[idx] as char;
        s.push(c);
    }
    s
}

/// Old interning approach using a per-batch HashMap<u64, Arc<str>> cache.
fn topk_old(inputs: &[&str], k: usize) {
    use std::collections::hash_map::DefaultHasher;
    let mut heap: BinaryHeap<Reverse<Arc<str>>> = BinaryHeap::new();
    let mut cache: HashMap<u64, Arc<str>> = HashMap::new();

    for &s in inputs {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let h = hasher.finish();

        // intern: check by hash, then verify equality to avoid collisions
        let arc = if let Some(a) = cache.get(&h) {
            if a.as_ref() == s {
                a.clone()
            } else {
                // hash collision, treat as miss
                let a = Arc::<str>::from(s);
                cache.insert(h, a.clone());
                a
            }
        } else {
            let a = Arc::<str>::from(s);
            cache.insert(h, a.clone());
            a
        };

        if heap.len() < k {
            heap.push(Reverse(arc));
        } else if arc.as_ref() > heap.peek().unwrap().0.as_ref() {
            heap.pop();
            heap.push(Reverse(arc));
        }
    }

    // consume heap
    let mut out = Vec::with_capacity(heap.len());
    while let Some(Reverse(v)) = heap.pop() {
        out.push(v.to_string());
    }
    out.reverse();
}

/// New compare-first approach: compare borrowed &str before allocating String only when replacing.
fn topk_new(inputs: &[&str], k: usize) {
    let mut heap: BinaryHeap<Reverse<String>> = BinaryHeap::new();

    for &s in inputs {
        if heap.len() < k {
            heap.push(Reverse(s.to_string()));
        } else if s > heap.peek().unwrap().0.as_str() {
            heap.pop();
            heap.push(Reverse(s.to_string()));
        }
    }

    // consume
    let mut out = Vec::with_capacity(heap.len());
    while let Some(Reverse(v)) = heap.pop() {
        out.push(v);
    }
    out.reverse();
}

fn bench_run(inputs: &[&str], k: usize, iterations: usize) -> (Duration, Duration) {
    // run a few warmups then measure
    for _ in 0..2 {
        topk_old(inputs, k);
        topk_new(inputs, k);
    }

    let mut t_old = Duration::ZERO;
    let mut t_new = Duration::ZERO;
    for _ in 0..iterations {
        let s = Instant::now();
        topk_old(inputs, k);
        t_old += s.elapsed();

        let s = Instant::now();
        topk_new(inputs, k);
        t_new += s.elapsed();
    }
    (t_old / (iterations as u32), t_new / (iterations as u32))
}

fn run_case(name: &str, inputs: &[String], k: usize) {
    let inputs_ref: Vec<&str> = inputs.iter().map(|s| s.as_str()).collect();

    println!("Case: {}  N={}  k={}", name, inputs_ref.len(), k);
    let (t_old, t_new) = bench_run(&inputs_ref, k, 5);
    println!(
        "  old(intern) = {:?}\n  new(compare) = {:?}\n  ratio old/new = {:.2}",
        t_old,
        t_new,
        t_old.as_secs_f64() / t_new.as_secs_f64()
    );
}

fn main() {
    // Accept optional seed and sizes via env or args
    let args: Vec<String> = env::args().collect();
    let seed: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(42);
    let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let k: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100);

    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    // Unique strings scenario
    let mut uniq = Vec::with_capacity(n);
    for _ in 0..n {
        uniq.push(gen_random_string(&mut rng, 16));
    }
    run_case("unique", &uniq, k);

    // Duplicates scenario: pick small unique set and repeat
    let mut small_set = Vec::with_capacity(1000);
    for _ in 0..1000 {
        small_set.push(gen_random_string(&mut rng, 12));
    }
    let mut dups = Vec::with_capacity(n);
    for i in 0..n {
        dups.push(small_set[i % small_set.len()].clone());
    }
    run_case("duplicates", &dups, k);

    // Heavy duplicates: only 50 uniques
    let mut tiny_set = Vec::with_capacity(50);
    for _ in 0..50 {
        tiny_set.push(gen_random_string(&mut rng, 8));
    }
    let mut heavy = Vec::with_capacity(n);
    for i in 0..n {
        heavy.push(tiny_set[i % tiny_set.len()].clone());
    }
    run_case("heavy_duplicates", &heavy, k);

    println!("Done");
}
