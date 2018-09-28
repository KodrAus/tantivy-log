/**
An example of a structured `log::Log` implementation that logs to tantivy.

This is a *really* simple logger, it's not designed to be efficient or
robust. Try playing with the records that are logged in `do_some_logging`
and pass different tantivy queries as the first argument to this binary
to see how records can be logged.
*/

mod logger;
mod searcher;
mod index;
mod schema;
mod store;

use log::{
    log,
    properties,
};

use serde_json::json;

use crate::{
    store::Store,
    searcher::Searcher,
};

pub type Error = failure::Error;

/**
Log some structured records to the store.
*/
fn do_some_logging(store: &Store) {
    logger::init(store.clone());

    log!(log::Level::Info, msg: { "A structured {name}", name = "log" }, kvs: {
        id: 1,
        #[log(display)]
        name: "log",
        #[log(path)]
        path: "./monkey-path",
    });

    log!(log::Level::Warn, msg: { "A structured {name}", name = "log" }, kvs: {
        id: 2,
        #[log(serde)]
        err: json!({
            "cause": "something went wrong!",
            "backtrace": [
                "line 1 ...",
                "line 2 ...",
                "line 3 ..."
            ]
        })
    });
}

/**
Query the store for some records.
*/
fn do_some_querying(store: &Store, query: &str) {
    let searcher = Searcher::new(store.clone());

    for doc in searcher.search(&query, 10).expect("failed to search") {
        println!("{}", doc.expect("failed to read doc"));
    }
}

fn main() {
    let store = Store::new();

    do_some_logging(&store);

    let query = std::env::args().skip(1).next().unwrap_or_else(|| "*".to_owned());

    println!("querying for `{}`", query);

    do_some_querying(&store, &query);
}
