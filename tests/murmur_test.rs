use rcmap::murmur::murmur;


#[test]
fn test_murmur() {
  let key = "hello".as_bytes().to_vec();
  let seed = 1u32;

  let hash = murmur(&key, seed);

  println!("hash {}", hash);
}