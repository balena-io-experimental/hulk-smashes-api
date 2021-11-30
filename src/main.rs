use clap::{App, Arg};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use reqwest::Client;
use std::{
    collections::{hash_map::Entry, HashMap},
    fs::File,
    io::{BufRead, BufReader},
    process,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const API_KEY_REQUESTS_PER_MINUTE: f32 = 500.0;
const AVERAGE_REQUEST_TIME: f32 = 0.1;

static PATCH_PAYLOAD: &str = r#"{
  "local": {
    "name": 'yeye-device',
    "is_online": true,
    "cpu_temp": 100000000000, // we cookin
    "memory_total": 100000000000000000,
    "memory_usage": 100000,
    "cpu_usage": 1000000000
  }
}"#;

#[tokio::main]
async fn main() {
    let args = App::new("HULKHULKHULK")
        .args(&[
            Arg::with_name("url").help("Base URL for API calls."),
            Arg::with_name("devices")
                .help("Path to a newline-separated list of '<deviceId> <apiKey>' pairs."),
            Arg::with_name("concurrency").help("Number of concurrent requests that should run."),
        ])
        .get_matches();

    let base_url = args.value_of("url").unwrap().to_owned();

    let concurrency = args
        .value_of("concurrency")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let min_n_api_keys =
        (API_KEY_REQUESTS_PER_MINUTE / 60.0 / AVERAGE_REQUEST_TIME).ceil() as usize;
    println!(
        "Will require a minimum of {} API keys assuming {:0.2} max requests per minute per API key and {:0.2} seconds average per request",
        min_n_api_keys, API_KEY_REQUESTS_PER_MINUTE, AVERAGE_REQUEST_TIME
    );

    println!("Reading devices...");
    let mut api_key_device_map = HashMap::new();
    for line in BufReader::new(File::open(args.value_of_os("devices").unwrap()).unwrap()).lines() {
        let line = line.unwrap();
        let (device_id, api_key) = line.split_at(line.find(' ').unwrap());
        let device_id = device_id.to_owned();
        match api_key_device_map.entry(api_key[1..].to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(vec![device_id]);
            }
            Entry::Occupied(mut entry) => entry.get_mut().push(device_id),
        }
    }
    let n_api_keys = api_key_device_map.len();
    println!("Found {} API keys", n_api_keys);

    if n_api_keys < min_n_api_keys {
        println!(
            "Not enough API keys to be able to reach concurrency of {} without running afoul or rate limits",
            concurrency
        );

        process::exit(1);
    }

    println!("Preparing...");

    drop(args);

    // Prepare for multithreaded execution
    let base_url = Arc::new(base_url);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let n_inflight_requests = Arc::new(AtomicUsize::new(0));
    let n_requests = Arc::new(AtomicUsize::new(0));
    let average_safe_request_time = Duration::from_secs_f32(API_KEY_REQUESTS_PER_MINUTE / 60.0);

    let client = Client::new();

    // Spawn one task per API key to make things simpler. Tasks are synchronized with a semaphore
    println!("Running");
    for (i, (api_key, mut device_ids)) in api_key_device_map.into_iter().enumerate() {
        let base_url = base_url.clone();
        let semaphore = semaphore.clone();
        let n_inflight_requests = n_inflight_requests.clone();
        let n_requests = n_requests.clone();
        let client = client.clone();

        tokio::spawn(async move {
            device_ids.shrink_to_fit();
            let mut rng = SmallRng::seed_from_u64(i as u64);

            loop {
                let _pass = semaphore.acquire().await;
                let start = Instant::now();

                let is_get = rng.gen_bool(0.5);
                let index = rng.gen_range(0..device_ids.len());

                let url = format!("{}/device/v2/{}/state", base_url, device_ids[index]);
                let request = if is_get {
                    client.get(url).bearer_auth(&api_key)
                } else {
                    client.patch(url).bearer_auth(&api_key).body(PATCH_PAYLOAD)
                };
                n_inflight_requests.fetch_add(1, Ordering::Release);
                match request.send().await {
                    Ok(response) => {
                        if !response.status().is_success() {
                            println!("Received status code {}", response.status())
                        }
                    }
                    Err(err) => println!("Error: {}", err),
                }
                n_inflight_requests.fetch_sub(1, Ordering::Release);
                n_requests.fetch_add(1, Ordering::Release);

                let end = Instant::now();
                let elapsed = end - start;
                if elapsed < average_safe_request_time {
                    tokio::time::sleep(average_safe_request_time - elapsed).await;
                }
            }
        });
    }

    // Status reporting
    let sleep_for = 30;
    loop {
        tokio::time::sleep(Duration::from_secs(sleep_for)).await;
        let n_inflight_requests = n_inflight_requests.load(Ordering::Acquire);
        let requests_per_second =
            (n_requests.swap(0, Ordering::Acquire) as f32) / (sleep_for as f32);
        println!(
            "{} requests in flight / {:0.2} requests per minute / {:0.2}s average per request",
            n_inflight_requests,
            requests_per_second * 60.0,
            (concurrency as f32) / requests_per_second
        );
    }
}
