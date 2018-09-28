# Structured logging with tantivy

This repository contains a simple example of a Rust structured logging sink that writes log events to [`tantivy`](https://github.com/tantivy-search/tantivy). It's meant as a demonstration of how structured logs can be captured, recorded, and queried.

Try:

```shell
$ cargo run -- "level:INFO"
```
