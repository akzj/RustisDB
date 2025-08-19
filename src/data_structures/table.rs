use std::collections::HashMap;

use crossbeam_skiplist::SkipList;


pub enum Value {
    Hash(HashMap<String, Value>)
}


pub struct MemTable {
    data: SkipList<String, Value>,
}
