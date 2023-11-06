use crate::consts::{BIT_CHUNK_SIZE, HASH_CHUNKS};


pub fn calculate_hamming_weight(bitmap: u32) -> u32 {
  bitmap.count_ones()
}

pub fn get_sparse_index(hash: u32, level: usize)  -> u32 {
  let updated_level = level % HASH_CHUNKS;
  get_index(hash, updated_level)
}

pub fn get_index(hash: u32, level: usize) -> u32 {
  let slots = 2u32.pow(BIT_CHUNK_SIZE as u32);
  let shift_size = slots - (BIT_CHUNK_SIZE * (level + 1)) as u32;
  let mask = slots - 1;
  
  hash >> shift_size & mask
}