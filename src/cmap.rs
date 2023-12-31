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
    loop {
      let root_ptr = self.root.load(Ordering::Acquire);
      let root_clone = unsafe { &mut *root_ptr };

      let path_copy = Self::put_recursive(root_clone, &key, &value, 0);
      let new_root_ptr = Box::into_raw(Box::new(path_copy));
      
      match self.root.compare_exchange_weak(root_ptr, new_root_ptr, Ordering::Relaxed, Ordering::Relaxed) {
        Ok(_) => { break; }
        Err(_) => {
          let _deallocated = unsafe { Box::from_raw(new_root_ptr) }; 
          continue; 
        }
      }
    }

    true
  }

  fn put_recursive(node: &CMapNode, key: &Vec<u8>, value: &Vec<u8>, level: usize) -> CMapNode {
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
        let mut child_node = unsafe { (*child_node_ptr).clone() };

        match child_node.is_leaf {
          true => {
            match key == &child_node.key {
              true => { 
                child_node.value = value.clone();
                
                let updated_child_node_ptr = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(child_node))));
                node_copy.children[position] = updated_child_node_ptr;
              }
              false => { 
                let mut new_internal_node = CMapNode::new_internal_node();

                new_internal_node = Self::put_recursive(&new_internal_node, &child_node.key, &child_node.value, level + 1);
                new_internal_node = Self::put_recursive(&new_internal_node, key, value, level + 1);
                
                let new_internal_node_ptr = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(new_internal_node))));
                node_copy.children[position] = new_internal_node_ptr;
              }
            }
          }
          false => { 
            child_node = Self::put_recursive(&child_node, key, value, level + 1); 
            
            let updated_child_node_ptr = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(child_node))));
            node_copy.children[position] = updated_child_node_ptr;
          }
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
        let child_node = unsafe { (*child_node_ptr).clone() };

        match child_node.is_leaf && key == &child_node.key {
          true => { return Some(child_node.value.to_vec()); }
          false => { return Self::get_recursive(&child_node, key, level + 1); }
        }
      }
    }
  }

  pub fn del(&self, key: Vec<u8>) -> bool {
    loop {
      let root_ptr = self.root.load(Ordering::Acquire);
      let root_clone = unsafe { (*root_ptr).clone() };
      let path_copy = Self::del_recursive(&root_clone, &key, 0);
      
      match path_copy {
        Some(path_copy) => {
          let new_root_ptr = Box::into_raw(Box::new(path_copy));
      
          match self.root.compare_exchange_weak(root_ptr, new_root_ptr, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => { break; }
            Err(_) => { continue; }
          }
        }
        None => { break; }
      }
    }

    true
  }

  fn del_recursive(node: &CMapNode, key: &Vec<u8>, level: usize) -> Option<CMapNode> {
    let hash = Self::calculate_hash_for_current_level(key, level);
    let index = get_sparse_index(hash, level);

    let mut node_copy = node.clone();

    match ! node_copy.is_bit_set(index) {
      true => { return None; }
      false => {
        let position = node_copy.get_position(hash, level);

        let child_node_ptr = node_copy.children[position].load(Ordering::Acquire);
        let child_node = unsafe { &*child_node_ptr };

        match child_node.is_leaf && key == &child_node.key {
          true => { 
            node_copy.set_bit(index);
            node_copy.shrink_table(position);

            return Some(node_copy);
          }
          false => {
            let node_on_path = Self::del_recursive(child_node, key, level + 1);

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

  pub fn print(&self) {
    let root_ptr = self.root.load(Ordering::Acquire);
    let root_clone = unsafe { (*root_ptr).clone() };

    Self::print_recursive(&root_clone, 0);
  }

  fn print_recursive(node: &CMapNode, level: usize) {
    println!("level {}", level);

    let node_children = &node.children;

    for (idx, child_node_arc) in node_children.into_iter().enumerate() {
      let child_node_ptr = child_node_arc.load(Ordering::Acquire);
      let child_node = unsafe { &*child_node_ptr };

      println!("idx {}, node {:?}", idx, child_node);
      Self::print_recursive(child_node, level + 1);
    }
  }

  fn calculate_hash_for_current_level(key: &Vec<u8>, level: usize) -> u32 {
    let curr_chunk = level / HASH_CHUNKS;
    let seed = (curr_chunk + 1) as u32;
  
    murmur(key, seed)
  }
}