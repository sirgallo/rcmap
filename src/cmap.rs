use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use crate::consts::HASH_CHUNKS;
use crate::murmur::murmur;
use crate::node::CMapNode;
use crate::utils::{calculate_hamming_weight, get_sparse_index};


#[derive(Debug)]
pub struct CMap {
  pub root: Arc<AtomicPtr<CMapNode>>, 
}

impl CMap {
  pub fn new() -> Self {
    let root_node = Box::new(CMapNode::new_internal_node());
    let root = Arc::new(AtomicPtr::new(Box::into_raw(root_node)));
    
    CMap { root }
  }

  pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
    let key_clone = key.clone();
    let value_clone = value.clone();

    loop {
      let root_ptr = self.root.load(Ordering::Acquire);
      let mut root_clone = unsafe { (*root_ptr).clone() };

      let path_copy = Self::put_recursive(&mut root_clone, &key_clone, &value_clone, 0);
      let new_root_ptr = Box::into_raw(Box::new(path_copy));
      
      match self.root.compare_exchange_weak(root_ptr, new_root_ptr, Ordering::Relaxed, Ordering::Relaxed) {
        Ok(_) => { 
          let _deallocated = unsafe { Box::from_raw(root_ptr) }; 
          break; 
        }
        Err(_) => {
          let _deallocated = unsafe { Box::from_raw(new_root_ptr) }; 
          continue; 
        }
      }
    }

    true
  }

  fn put_recursive(node: &mut CMapNode, key: &Vec<u8>, value: &Vec<u8>, level: usize) -> CMapNode {
    let hash = Self::calculate_hash_for_current_level(key, level);
    let index = get_sparse_index(hash, level);

    let mut node_copy = node.clone();
    let position = node_copy.get_position(hash, level);

    match ! node_copy.is_bit_set(index) {
      true => {
        let new_leaf = CMapNode::new_leaf_node(key, value);
        
        node_copy.set_bit(index);
        node_copy.extend_table(position, new_leaf);
      }
      false => {
        let child_node_ptr = node_copy.children[position].load(Ordering::Acquire);
        let mut child_node_copy = unsafe { (*child_node_ptr).clone() };

        match child_node_copy.is_leaf {
          true => {
            match key == &child_node_copy.key {
              true => { 
                child_node_copy.value = value.clone();
                node_copy.children[position] = Arc::new(AtomicPtr::new(child_node_ptr));
              }
              false => { 
                let mut new_internal_node = CMapNode::new_internal_node();

                new_internal_node = Self::put_recursive(&mut new_internal_node, &child_node_copy.key, &child_node_copy.value, level + 1);
                new_internal_node = Self::put_recursive(&mut new_internal_node, key, value, level + 1);
                
                let new_internal_node_ptr = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(new_internal_node))));
                node_copy.children[position] = new_internal_node_ptr;
              }
            }
          }
          false => { Self::put_recursive(&mut child_node_copy, key, value, level + 1); }
        }
      }
    }

    node_copy
  }

  pub fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
    let root_ptr = self.root.load(Ordering::Acquire);
    let root_clone = unsafe { (*root_ptr).clone() };

    return Self::get_recursive(&root_clone, &key, 0);
  }

  fn get_recursive(node: &CMapNode, key: &Vec<u8>, level: usize) -> Option<Vec<u8>> {
    let hash = Self::calculate_hash_for_current_level(key, level);
    let index = get_sparse_index(hash, level);

    match ! node.is_bit_set(index) {
      true => { return None; }
      false => {
        let position = node.get_position(hash, level);
        let child_node_ptr = node.children[position].load(Ordering::Acquire);
        let child_node_copy = unsafe { (*child_node_ptr).clone() };

        match child_node_copy.is_leaf && key == &child_node_copy.key {
          true => { return Some(child_node_copy.value.clone()); }
          false => { return Self::get_recursive(&child_node_copy, key, level + 1); }
        }
      }
    }
  }

  pub fn del(&self, key: Vec<u8>) -> bool {
    let key_clone = key.clone();

    loop {
      let root_ptr = self.root.load(Ordering::Acquire);
      let mut root_clone = unsafe { (*root_ptr).clone() };
      let path_copy = Self::del_recursive(&mut root_clone, &key_clone, 0);
      
      match path_copy {
        Some(path_copy) => {
          let new_root_ptr = Box::into_raw(Box::new(path_copy));
      
          match self.root.compare_exchange_weak(root_ptr, new_root_ptr, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => { break; }
            Err(_) => { continue; }
          }
        }
        None => { continue; }
      }
    }

    true
  }

  fn del_recursive(node: &mut CMapNode, key: &Vec<u8>, level: usize) -> Option<CMapNode> {
    let hash = Self::calculate_hash_for_current_level(key, level);
    let index = get_sparse_index(hash, level);

    let mut node_copy = node.clone();

    match ! node_copy.is_bit_set(index) {
      true => { return None; }
      false => {
        let position = node_copy.get_position(hash, level);

        let child_node_ptr = node_copy.children[position].load(Ordering::Acquire);
        let mut child_node_copy = unsafe { (*child_node_ptr).clone() };

        match child_node_copy.is_leaf && key == &child_node_copy.key {
          true => { 
            node_copy.set_bit(index);
            node_copy.shrink_table(position);

            return Some(node_copy);
          }
          false => {
            let node_on_path = Self::del_recursive(&mut child_node_copy, key, level + 1);

            match node_on_path {
              Some(new_node) => { 
                if ! new_node.is_leaf && calculate_hamming_weight(new_node.bitmap) == 0 {
                  node_copy.set_bit(index);
                  node_copy.shrink_table(position);
                }

                return Some(node_copy);
              }
              None => { return None; }
            }
          }
        }
      }
    }
  }

  fn calculate_hash_for_current_level(key: &Vec<u8>, level: usize) -> u32 {
    let curr_chunk = level / HASH_CHUNKS;
    let seed = (curr_chunk + 1) as u32;
  
    murmur(key.clone(), seed)
  }
}