use rand::{RngCore, Error};

use rcmap::cmap::CMap;


const S_INPUT_SIZE: usize = 1000000;


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
fn test_singlecore_cmap() {
  let test_data = setup_test_data(S_INPUT_SIZE);
  let cmap = CMap::new();

  let put_time = std::time::Instant::now();

  for kv in &test_data {
    let kv_clone = kv.clone();
    let put_ok = cmap.put(kv_clone.key, kv_clone.value);
    // println!("is ok? {}", putok);
    assert!(put_ok);
  }

  let put_duration = put_time.elapsed();
  println!("Time for put operations: {:?}", put_duration);

  let get_time = std::time::Instant::now();

  for kv in &test_data {
    let kv_clone = kv.clone();
    let val = cmap.get(kv_clone.key.clone());
    match val {
      Some(exists) => {
        // println!("val {:?}", exists);
        assert!(exists == kv_clone.value);
      }
      None => { panic!("value in map was none type") }
    }
  }

  let get_duration = get_time.elapsed();
  println!("Time for get operations: {:?}", get_duration);

  let del_time = std::time::Instant::now();
  
  for kv in &test_data {
    let kv_clone = kv.clone();
    let del_ok = cmap.del(kv_clone.key);
    assert!(del_ok);
  }

  let del_duration = del_time.elapsed();
  println!("Time for del operations: {:?}", del_duration);
}