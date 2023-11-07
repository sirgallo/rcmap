use std::sync::Arc;
use std::thread::spawn;
use rand::{RngCore, Error};

use rcmap::cmap::CMap;

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
  let test_data = setup_test_data(100000);
  let test_data_arc = Arc::new(test_data);
  let cmap = Arc::new(CMap::new());

  let put_handles: Vec<_> = test_data_arc.iter().map(|kv| {
    let cmap_clone = Arc::clone(&cmap);
    let kv_clone = kv.clone();
    
    spawn(move || {
      let put_ok = cmap_clone.put(kv_clone.key, kv_clone.value);
      // println!("is ok? {}", putok);
      assert!(put_ok);
    })
  }).collect();

  for put_handle in put_handles {
    put_handle.join().expect("thread panicked!")
  }

  // cmap.print();

  let get_handles = test_data_arc.iter().map(|kv| {
    let cmap_clone = Arc::clone(&cmap);
    let key_clone = kv.clone();

    spawn(move || {
      let val = cmap_clone.get(key_clone.key.clone());
      match val {
        Some(exists) => {
          // println!("val {:?}", exists);
          assert!(exists == key_clone.value);
        }
        None => { panic!("value in map was none type") }
      }
    })
  });

  for get_handle in get_handles {
    get_handle.join().expect("Thread panicked");
  }

  let del_handles = test_data_arc.iter().map(|kv| {
    let cmap_clone = Arc::clone(&cmap);
    let key_clone = kv.clone();

    spawn(move || {
      let del_ok = cmap_clone.del(key_clone.key);
      assert!(del_ok);
    })
  });

  for del_handle in del_handles {
    del_handle.join().expect("Thread panicked");
  }
}