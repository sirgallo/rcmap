const C32_1: u32 = 0x85ebca6b;
const C32_2: u32 = 0xc2b2ae35;
const C32_3: u32 = 0xe6546b64;
const C32_4: u32 = 0x1b873593;
const C32_5: u32 = 0x5c4bcea9;


pub fn murmur(data: &Vec<u8>, seed: u32) -> u32 {
  let mut hash = seed;
  let length = data.len() as u32;
  let tot_four_byte_chunks = length / 4;

  for i in 0..tot_four_byte_chunks {
    let start_idx_of_chunk = (i * 4) as usize;
    let end_idx_of_chunk = ((i + 1) * 4) as usize;
    
    let bytes = &data[start_idx_of_chunk..end_idx_of_chunk];
    let chunk = u32::from_le_bytes(bytes.try_into().expect("vector of incorrect size"));
    let rotated_chunk = chunk.wrapping_mul(C32_1).rotate_right(17).wrapping_mul(C32_2);

    hash ^= rotated_chunk;
    hash = hash.rotate_right(19).wrapping_mul(5).wrapping_add(C32_3);
  }

  let remaining= &data[data.len() - data.len() % 4..];
  let mut chunk = 0u32;

  if remaining.len() >= 3 { chunk |= (remaining[2] as u32) << 16; }
  if remaining.len() >= 2 { chunk |= (remaining[1] as u32) << 8; }
  if remaining.len() >= 1 {
    chunk |= remaining[0] as u32;
    chunk = chunk.wrapping_mul(C32_1).rotate_right(17).wrapping_mul(C32_2);

    hash ^= chunk;
  }
  
  hash ^= length;
  hash ^= hash >> 16;
  hash = hash.wrapping_mul(C32_4);
  hash ^= hash >> 13;
  hash = hash.wrapping_mul(C32_5);
  hash ^= hash >> 16;

  return hash
}