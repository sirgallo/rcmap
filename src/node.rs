use std::sync::Arc;
use std::sync::atomic::AtomicPtr;

use crate::utils::{calculate_hamming_weight, get_sparse_index};


#[derive(Clone)]
#[derive(Debug)]
pub struct CMapNode {
  pub is_leaf: bool,
  pub bitmap: u32,
  pub children: Vec<Arc<AtomicPtr<CMapNode>>>,
  pub key: Vec<u8>,
  pub value: Vec<u8>
}

impl CMapNode {
  pub fn new_internal_node() -> Self {
    CMapNode { 
      is_leaf: false,
      bitmap: 0u32,
      children: Vec::new(),
      key: Vec::new(),
      value: Vec::new()
    }
  }

  pub fn new_leaf_node(key: &Vec<u8>, value: &Vec<u8>) -> Self {
    CMapNode {
      is_leaf: true,
      bitmap: 0u32,
      children: Vec::new(),
      key: key.clone(),
      value: value.clone()
    }
  }

  pub fn get_position(&self, hash: u32, level: usize) -> usize {
    let sparse_idx = get_sparse_index(hash, level);
    let mask = (1 << sparse_idx) - 1;
    let isolated_bits = self.bitmap & mask;
    
    calculate_hamming_weight(isolated_bits) as usize
  }

  pub fn is_bit_set(&self, position: u32) -> bool {
    (self.bitmap & (1 << position)) != 0
  }

  pub fn set_bit(&mut self, position: u32) {
    self.bitmap = self.bitmap ^ (1 << position);
  }
  
  pub fn extend_table(&mut self, position: usize, new_node: CMapNode) {
    let table_size = calculate_hamming_weight(self.bitmap) as usize;
    let mut new_table = Vec::with_capacity(table_size);

    for existing_arc in self.children.iter().take(position) {
      new_table.push(existing_arc.clone());
    }

    new_table.push(Arc::new(AtomicPtr::new(Box::into_raw(Box::new(new_node)))));
    
    for existing_arc in self.children.iter().skip(position) {
      new_table.push(existing_arc.clone());
    }

    self.children = new_table;
  }
  
  pub fn shrink_table(&mut self, position: usize) {
    let table_size = calculate_hamming_weight(self.bitmap) as usize;
    let mut new_table = Vec::with_capacity(table_size);

    for existing_arc in self.children.iter().take(position) {
      new_table.push(existing_arc.clone());
    }

    for existing_arc in self.children.iter().skip(position + 1) {
      new_table.push(existing_arc.clone())
    }
  
    self.children = new_table;
  }
}