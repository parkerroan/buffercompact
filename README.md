# Buffer Compactor Lib

Buffer Compactor is a tool to allow for buffering for a duration and compacting data on keys. It uses a badgerDB and sortedset in order to coridinate a time-delayed queue that also aggregates updates sharing the same key in a extremely peformant manner. 

BufferCompactor is thread safe and be can shared by consumer and producer threads to delay kafka topics or other message like workloads.

BadgerDB can be configured to live completely on RAM or on Disk depending on workloads. 

## Example Usage: 

### Getting Started:
Not handling errors to save space in example
```go
package main

import (
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/parkerroan/buffercompact"
	"github.com/parkerroan/buffercompact/sortedset"
)

func main() {
	sortedset := sortedset.New()
	db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	bufferDuration := 1 * time.Second

	buffcomp, _ := buffercompact.New(db, sortedset, bufferDuration, buffercompact.WithMaxValues(5))

	buffcomp.StoreToQueue("test1", []byte("testValue1"))
	buffcomp.StoreToQueue("test1", []byte("testValue2"))

	items, _ := buffcomp.RetreiveFromQueue(10)
	fmt.Printf("No Results: %v \n", items)

	time.Sleep(1 * time.Second)

	items, _ = buffcomp.RetreiveFromQueue(10)
	fmt.Printf("Results After Buffer Duration: %v \n", items)

	//Insert More Records Up To Max
	buffcomp.StoreToQueue("test1", []byte("testValue1"))
	buffcomp.StoreToQueue("test2", []byte("testValue2"))
	buffcomp.StoreToQueue("test3", []byte("testValue3"))
	buffcomp.StoreToQueue("test4", []byte("testValue4"))
	buffcomp.StoreToQueue("test5", []byte("testValue5"))
	buffcomp.StoreToQueue("test6", []byte("testValue6"))

	items, _ = buffcomp.RetreiveFromQueue(10)
	fmt.Printf("All Results Returned Due To Max Values Limit (Ignored Buffer Duration): %v \n", items)
}

```

```go
# github.com/parkerroan/bufffercompact/examples/getting_started.go
```
