package main

import (
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/parkerroan/buffercompact"
)

func main() {
	db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	bufferDuration := 1 * time.Second

	buffcomp, _ := buffercompact.New(db, bufferDuration, buffercompact.WithMaxValueCount(5))

	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test1", Value: []byte("testValue1")})
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test1", Value: []byte("testValue2")})

	limit := 10
	items, _ := buffcomp.RetrieveFromQueue(limit)
	fmt.Printf("No Results: %v \n", items)

	time.Sleep(1 * time.Second)

	items, _ = buffcomp.RetrieveFromQueue(limit)
	fmt.Printf("Results After Buffer Duration: %v \n", items)

	//Insert More Records Up To Max
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test1", Value: []byte("testValue1")})
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test2", Value: []byte("testValue2")})
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test3", Value: []byte("testValue3")})
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test4", Value: []byte("testValue4")})
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test5", Value: []byte("testValue5")})
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test6", Value: []byte("testValue6")})

	items, _ = buffcomp.RetrieveFromQueue(limit)
	fmt.Printf("All Results Returned Due To Max Values Limit (Ignored Buffer Duration): %v \n", items)

	//DEDUPE SECTION
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test1", Value: []byte("testValue1"), UniqueID: "unique-instance-identifier-or-content-hash"})
	time.Sleep(2 * time.Second)
	items, _ = buffcomp.RetrieveFromQueue(limit)
	fmt.Printf("The first time it was treated normally here: %v \n", items)

	//Insert same data again in future...
	buffcomp.StoreToQueue(buffercompact.StorageItem{Key: "test1", Value: []byte("testValue1"), UniqueID: "unique-instance-identifier-or-content-hash"})
	time.Sleep(2 * time.Second)
	items, _ = buffcomp.RetrieveFromQueue(limit)
	fmt.Printf("This time it was ignored as it was already seen: %v \n", items)
}
