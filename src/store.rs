use std::{
    sync::{
        Arc,
        Mutex,
    },
    collections::HashMap,
};

use tantivy::{
    Index,
    IndexWriter,
};

use crate::{
    index::IndexId,
    schema::IndexableDoc,
};

const HEAP_SIZE: usize = 50_000_000;

#[derive(Clone)]
pub struct Store {
    state: Arc<Mutex<HashMap<IndexId, Index>>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            state: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn get_writer(&self, doc: &IndexableDoc) -> Result<IndexWriter, crate::Error> {
        let mut state = self.state.lock().expect("poisoned state");

        if let Some(ref index) = state.get(&doc.index) {
            let writer = index.writer(HEAP_SIZE)?;

            return Ok(writer);
        }

        let index = Index::create_in_ram(doc.schema.clone());
        let writer = index.writer(HEAP_SIZE)?;

        state.insert(doc.index, index);

        Ok(writer)
    }

    pub fn indexes(&self) -> impl IntoIterator<Item = (IndexId, Index)> {
        self.state.lock().expect("poisoned state").clone()
    }
}