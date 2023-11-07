use std::sync::Arc;
use std::thread::{spawn, JoinHandle};
use rand::{RngCore, Error};

use rcmap::cmap::CMap;


const NUM_CPU: usize = 2;
const P_INPUT_SIZE: usize = 1000000;
const P_CHUNK_SIZE: usize = P_INPUT_SIZE / NUM_CPU;


#[derive(Clone)]
struct KeyVal {
	pub key: Vec<u8>,
	pub value: Vec<u8>
}


fn generate_random_bytes(length: usize) -> Result<Vec<u8>, Error> {
  let mut rng = rand::thread_rng();
  let mut random_bytes = vec![0u8; length];
  rng.try_fill_bytes(&mut random_bytes)?;
  Ok(random_bytes)
}

fn setup() -> (Arc<CMap>, Vec<KeyVal>, Vec<KeyVal>) {
  let cmap = Arc::new(CMap::new());
  
  let mut init_key_val_pairs = Vec::with_capacity(P_INPUT_SIZE);
  let mut p_key_val_pairs = Vec::with_capacity(P_INPUT_SIZE);

  for _ in 0..P_INPUT_SIZE {
    let key = generate_random_bytes(32).expect("Failed to generate key");
    let value = key.clone(); // Using the same random bytes for value in this case

    init_key_val_pairs.push(KeyVal { key: key.clone(), value: value.clone() });
    cmap.put(key.clone(), value.clone());
  }

  for _ in 0..P_INPUT_SIZE {
    let key = generate_random_bytes(32).expect("Failed to generate key");
    let value = key.clone();

    p_key_val_pairs.push(KeyVal { key, value });
  }

  (cmap, init_key_val_pairs, p_key_val_pairs)
}


#[test]
fn test_parallel_cmap() {
  let (cmap, init_key_val_pairs, p_key_val_pairs) = setup();
  
  let mut handles: Vec<JoinHandle<()>> = vec![];

  let start_time = std::time::Instant::now();

  for i in 0..NUM_CPU {
    let cmap_clone = Arc::clone(&cmap);
    let p_key_val_pairs_chunk = p_key_val_pairs[i * P_CHUNK_SIZE..(i + 1) * P_CHUNK_SIZE].to_vec();

    let handle = spawn(move || {
      for kv in p_key_val_pairs_chunk {
        let kv_clone = kv.clone();
        let put_ok = cmap_clone.put(kv_clone.key, kv_clone.value);
        assert!(put_ok);
      }
    });

    handles.push(handle);
  }

  for i in 0..NUM_CPU {
    let cmap_clone = Arc::clone(&cmap);
    let init_key_val_pairs_chunk = init_key_val_pairs[i * 1000..(i + 1) * 1000].to_vec();

    let handle = spawn(move || {
      for kv in init_key_val_pairs_chunk {
        let kv_clone = kv.clone();
        let val = cmap_clone.get(kv_clone.key);
        match val {
          Some(exists) => {
            assert!(exists == kv_clone.value);
          }
          None => { panic!("value in map was none type") }
        }
      }
    });

    handles.push(handle);
  }

  for handle in handles {
    handle.join().unwrap();
  }

  let duration = start_time.elapsed();
  println!("Time for parallel read/write operations: {:?}", duration);
}