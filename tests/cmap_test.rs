use rcmap::cmap::CMap;


#[test]
fn test_cmap() {
  let map = CMap::new();

  let put_ok1 = map.put("hello".as_bytes().to_vec(), "world".as_bytes().to_vec());
  assert!(put_ok1);

  let put_ok2 = map.put("new".as_bytes().to_vec(), "wow!".as_bytes().to_vec());
  assert!(put_ok2);

  let val1 = map.get("hello".as_bytes().to_vec());
  match val1 {
    Some(val) => {
      assert!(val == "world".as_bytes().to_vec());
    }
    None => { panic!("value in map was none type") } 
  }

  let val2 = map.get("new".as_bytes().to_vec());
  match val2 {
    Some(val) => {
      assert!(val == "wow!".as_bytes().to_vec());
    }
    None => { panic!("value in map was none type") } 
  }

  let del_ok = map.del("hello".as_bytes().to_vec());
  assert!(del_ok);
}