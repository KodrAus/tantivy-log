use std::{
    collections::HashMap,
};

use tantivy::{
    IndexWriter,
};

use serde::Serialize;

use crate::{
    schema::Doc,
    store::Store,
};

pub type IndexId = u64;

/**
An indexer for a store.
*/
pub struct Indexer {
    store: Store,
    writers: HashMap<IndexId, IndexWriter>,
}

impl Indexer {
    pub fn new(store: Store) -> Self {
        Indexer {
            store,
            writers: HashMap::new()
        }
    }

    pub fn index(&mut self, doc: impl Serialize) -> Result<(), crate::Error> {
        let doc = Doc::build(doc)?;

        if let Some(ref mut writer) = self.writers.get_mut(&doc.index()) {
            let i = doc.indexable();

            writer.add_document(i.doc);
            writer.commit()?;
        } else {
            let i = doc.indexable();

            let mut writer = self.store.get_writer(&i)?;

            writer.add_document(i.doc);
            writer.commit()?;

            self.writers.insert(doc.index().to_owned(), writer);
        }

        Ok(())
    }
}
