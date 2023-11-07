use std::sync::Arc;
use std::thread::{spawn, JoinHandle};
use rand::{RngCore, Error};

use rcmap::cmap::CMap;


const NUM_CPU: usize = 4;
const C_INPUT_SIZE: usize = 1000000;
const C_CHUNK_SIZE: usize = C_INPUT_SIZE / NUM_CPU;


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

fn setup_test_data(size: usize) -> Vec<KeyVal> {
  (0..size)
    .map(|_| {
      let key = generate_random_bytes(32).expect("Failed to generate key");
      let value = key.clone(); // Using the same random bytes for value in this case
      KeyVal { key, value }
    })
    .collect()
}


#[test]
fn test_concurrent_cmap() {
  let test_data = setup_test_data(C_INPUT_SIZE);
  let cmap = Arc::new(CMap::new());

  let mut put_handles: Vec<JoinHandle<()>> = Vec::new();
  let mut get_handles: Vec<JoinHandle<()>> = Vec::new();
  let mut del_handles: Vec<JoinHandle<()>> = Vec::new();

  let put_time = std::time::Instant::now();
  
  for i in 0..NUM_CPU {
    let cmap_clone = Arc::clone(&cmap);
    let data_chunk = test_data[i * C_CHUNK_SIZE..(i + 1) * C_CHUNK_SIZE].to_vec();

    let handle = spawn(move || {
      for kv in data_chunk {
        let kv_clone = kv.clone();
        let put_ok = cmap_clone.put(kv_clone.key, kv_clone.value);
        // println!("is ok? {}", putok);
        assert!(put_ok);
      }
    });

    put_handles.push(handle);
  }
  
  for put_handle in put_handles {
    put_handle.join().expect("thread panicked!")
  }

  let put_duration = put_time.elapsed();
  println!("Time for put operations: {:?}", put_duration);

  // cmap.print();

  let get_time = std::time::Instant::now();

  for i in 0..NUM_CPU {
    let cmap_clone = Arc::clone(&cmap);
    let data_chunk = test_data[i * C_CHUNK_SIZE..(i + 1) * C_CHUNK_SIZE].to_vec();

    let handle = spawn(move || {
      for kv in data_chunk {
        let kv_clone = kv.clone();
        let val = cmap_clone.get(kv_clone.key.clone());
        match val {
          Some(exists) => {
            // println!("val {:?}", exists);
            assert!(exists == kv_clone.value);
          }
          None => { panic!("value in map was none type") }
        }
      }
    });

    get_handles.push(handle);
  }

  for get_handle in get_handles {
    get_handle.join().expect("Thread panicked");
  }

  let get_duration = get_time.elapsed();
  println!("Time for get operations: {:?}", get_duration);

  let del_time = std::time::Instant::now();

  for i in 0..NUM_CPU {
    let cmap_clone = Arc::clone(&cmap);
    let data_chunk = test_data[i * C_CHUNK_SIZE..(i + 1) * C_CHUNK_SIZE].to_vec();

    let handle = spawn(move || {
      for kv in data_chunk {
        let kv_clone = kv.clone();
        let del_ok = cmap_clone.del(kv_clone.key);
        assert!(del_ok);
      }
    });

    del_handles.push(handle);
  }

  for del_handle in del_handles {
    del_handle.join().expect("Thread panicked");
  }

  let del_duration = del_time.elapsed();
  println!("Time for del operations: {:?}", del_duration);
}