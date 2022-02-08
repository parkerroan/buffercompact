# Buffer Compactor Lib

Buffer Compactor is a tool to allow for buffering for a duration and compacting data on keys. It uses a badgerDB and sortedset in order to coridinate a time-delayed queue that also aggregates updates sharing the same key. 

BufferCompactor is thread safe and can shared by consumer and producer threads to delay kafka topics or other message like workloads. 

## Example Usage: 

### Inserting Data:
Not handling errors to save space in example
```

    sortedset := sortedset.New()
    db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
    bufferDuration := 1 * time.Second

    buffcomp, _ := New(db, sortedset, bufferDuration)

    buffcomp.StoreToQueue("test1", []byte("testValue1"))
```

### Retrieving Data:
Not handling errors to save space in example
```

    sortedset := sortedset.New()
    db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
    bufferDuration := 1 * time.Second

    buffcomp, _ := New(db, sortedset, bufferDuration)

    buffcomp.StoreToQueue("test1", []byte("testValue1"))
    buffcomp.StoreToQueue("test1", []byte("testValue2"))

    items, _ := buffcomp.RetrieveFromQueue(10)
    fmt.Printf("No Results: %v", items)

    time.sleep(1 * time.Second)

    items, _ := buffcomp.RetrieveFromQueue(10)
    fmt.Printf("Results After Buffer Duration: %v", items)
```

