use std::{
    cmp::Ordering,
    collections::{
        BinaryHeap,
        HashMap,
    },
};

use tantivy::{
    query::QueryParser,
    collector::Collector,
    Score,
    DocAddress,
    SegmentLocalId,
    SegmentReader,
    DocId,
    TantivyError,
};

use failure;

use crate::{
    index::IndexId,
    store::Store
};

/**
A searcher over the store.

The searcher will look in all indexes.
*/
pub struct Searcher {
    store: Store,
}

impl Searcher {
    pub fn new(store: Store) -> Self {
        Searcher {
            store,
        }
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<impl IntoIterator<Item = Result<String, crate::Error>>, crate::Error> {
        let mut lookup = HashMap::new();
        let mut collector = MultiIndexCollector::with_limit(limit);

        // We collect results from all indexes into a single collector
        for (id, index) in self.store.indexes() {
            let mut collector = CurrentIndexCollector::begin(id.to_owned(), &mut collector);

            index.load_searchers()?;
            let searcher = index.searcher();

            let query_parser = QueryParser::for_index(&index, vec![]);
            let query = query_parser.parse_query(query).map_err(|e| failure::err_msg(format!("{:?}", e)))?;

            searcher.search(&*query, &mut collector)?;

            lookup.insert(id, (index, searcher));
        }

        Ok(collector.top_docs().into_iter().map(move |doc| {
            let (ref index, ref searcher) = lookup[&doc.index];

            let doc = searcher.doc(doc.address)?;
            Ok(index.schema().to_json(&doc))
        }))
    }
}

// NOTE: These types are pinched from tantivy directly
// They've been tweaked to support an extra `IndexId` field

#[derive(Clone, Copy)]
struct Doc {
    score: Score,
    index: IndexId,
    address: DocAddress,
}

impl PartialOrd for Doc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Doc {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or_else(|| other.address.cmp(&self.address))
    }
}

impl PartialEq for Doc {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Doc {}

struct MultiIndexCollector {
    limit: usize,
    heap: BinaryHeap<Doc>,
    segment_id: u32,
}

impl MultiIndexCollector {
    fn with_limit(limit: usize) -> MultiIndexCollector {
        if limit < 1 {
            panic!("Limit must be strictly greater than 0.");
        }

        MultiIndexCollector {
            limit,
            heap: BinaryHeap::with_capacity(limit),
            segment_id: 0,
        }
    }

    fn top_docs(&self) -> impl IntoIterator<Item = Doc> {
        let mut feature_docs: Vec<Doc> = self.heap.iter().cloned().collect();
        feature_docs.sort();
        feature_docs
    }

    #[inline]
    fn at_capacity(&self) -> bool {
        self.heap.len() >= self.limit
    }

    fn set_segment_id(&mut self, segment_id: SegmentLocalId) {
        self.segment_id = segment_id;
    }

    fn collect(&mut self, index: IndexId, doc: DocId, score: Score) {
        if self.at_capacity() {
            // It's ok to unwrap as long as a limit of 0 is forbidden.
            let limit_doc: Doc = self
                .heap
                .peek()
                .expect("Collector with size 0 is forbidden")
                .clone();
            if limit_doc.score < score {
                let mut mut_head = self
                    .heap
                    .peek_mut()
                    .expect("Collector with size 0 is forbidden");
                mut_head.score = score;
                mut_head.address = DocAddress(self.segment_id, doc);
            }
        } else {
            let wrapped_doc = Doc {
                score,
                index,
                address: DocAddress(self.segment_id, doc),
            };
            self.heap.push(wrapped_doc);
        }
    }
}

struct CurrentIndexCollector<'a> {
    index: IndexId,
    collector: &'a mut MultiIndexCollector,
}

impl<'a> CurrentIndexCollector<'a> {
    fn begin(index: IndexId, collector: &'a mut MultiIndexCollector) -> Self {
        CurrentIndexCollector {
            index,
            collector,
        }
    }
}

impl<'a> Collector for CurrentIndexCollector<'a> {
    fn set_segment(&mut self, segment_id: SegmentLocalId, _: &SegmentReader) -> Result<(), TantivyError> {
        self.collector.set_segment_id(segment_id);
        Ok(())
    }

    fn collect(&mut self, doc: DocId, score: Score) {
        self.collector.collect(self.index, doc, score);
    }

    fn requires_scoring(&self) -> bool {
        true
    }
}
