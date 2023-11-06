use rcmap::cmap::CMap;

#[test]
fn test_cmap() {
  let map = CMap::new();

  let put_ok = map.put("hello".as_bytes().to_vec(), "world".as_bytes().to_vec());
  assert!(put_ok);

  let val1 = map.get("hello".as_bytes().to_vec());
  match val1 {
    Some(val) => {
      assert!(val == "world".as_bytes().to_vec());
    }
    None => { panic!("value in map was none type") } 
  }

  let del_ok = map.del("hello".as_bytes().to_vec());
  assert!(del_ok);
}