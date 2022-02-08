# Buffer Compactor Lib

Buffer Compactor is a tool to allow for buffering for a duration and compacting data on keys. It uses a badgerDB and sortedset in order to coridinate a time-delayed queue that also aggregates updates sharing the same key. 

BufferCompactor is thread safe and can shared by consumer and producer threads to delay kafka topics or other message like workloads. 

## Example Usage: 

### Getting Started:
Not handling errors to save space in example
```go:examples/getting_started.go

```
