use std::sync::Mutex;

use log::{
    set_boxed_logger,
    set_max_level,
    LevelFilter,
    Log,
    Record,
    Level,
    Metadata,
    key_values::{
        IntoMap,
    },
};

use serde_derive::Serialize;

use crate::{
    index::Indexer,
    store::Store,
};

/**
An implementation of `Log` that writes to `tantivy`.

This logger will flush after each event. This isn't really ideal,
but since we only log to a RAM drive it's not a big deal.
*/
pub struct Logger {
    indexer: Mutex<Indexer>,
}

impl Log for Logger {
    fn log(&self, record: &Record) {
        let mut indexer = self.indexer.lock().expect("indexer poisoned");

        let record = IndexableRecord {
            level: record.level(),
            msg: format!("{}", record.args()),
            props: record.key_values().into_map(),
        };

        let _ = indexer.index(record);
    }

    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn flush(&self) {

    }
}

/**
A log record that can be serialized and indexed.

It's built from a standard `log::Record`.
*/
#[derive(Serialize)]
struct IndexableRecord<KVS> {
    level: Level,
    msg: String,
    props: KVS,
}

pub fn init(store: Store) {
    set_boxed_logger(Box::new(Logger {
        indexer: Mutex::new(Indexer::new(store)),
    })).expect("failed to init logger");

    set_max_level(LevelFilter::Info);
}
