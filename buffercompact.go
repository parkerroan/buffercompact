// Copyright (c) 2022, Parker.Roan
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//  list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//  this list of conditions and the following disclaimer in the documentation
//  and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package buffercompact

import (
	"errors"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/parkerroan/buffercompact/sortedset"
)

var (
	ErrMaxValueCount = errors.New("max value count reached")
)

type BufferCompactor struct {
	db             *badger.DB
	sortedSet      *sortedset.SortedSet
	bufferDuration time.Duration
	mu             sync.Mutex

	maxValuesCount int
}

type BufferCompactorOption func(*BufferCompactor)

type StorageItem struct {
	Key   string
	Value []byte
}

func New(db *badger.DB, sortedSet *sortedset.SortedSet, bufferDuration time.Duration, opts ...BufferCompactorOption) (*BufferCompactor, error) {
	byffComp := BufferCompactor{
		db:             db,
		sortedSet:      sortedSet,
		bufferDuration: bufferDuration,
	}

	for _, opt := range opts {
		opt(&byffComp)
	}

	return &byffComp, nil
}

func WithMaxValueCount(maxLength int) BufferCompactorOption {
	return func(b *BufferCompactor) {
		b.maxValuesCount = maxLength
	}
}

func (b *BufferCompactor) StoreToQueue(key string, value []byte) error {
	if b.maxValuesCount != 0 && b.sortedSet.GetCount() >= b.maxValuesCount {
		return ErrMaxValueCount
	}

	err := b.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), value)
		return err
	})
	if err != nil {
		return err
	}

	if node := b.sortedSet.GetByKey(key); node == nil {
		score := sortedset.SCORE(time.Now().Add(b.bufferDuration).Unix())
		b.sortedSet.AddOrUpdate(key, score, struct{}{})
	}
	return nil
}

func (b *BufferCompactor) RetreiveFromQueue(limit int) ([]*StorageItem, error) {
	var nodes []*sortedset.SortedSetNode

	//lock here to allow for multiple caller threads
	b.mu.Lock()
	//if max set length is hit, aggressively remove items disregarding
	//buffer duration
	if b.maxValuesCount != 0 && b.sortedSet.GetCount() >= b.maxValuesCount {
		nodes = b.sortedSet.GetByRankRange(1, limit, true)
	} else {
		end := sortedset.SCORE(time.Now().Unix())
		nodes = b.sortedSet.GetByScoreRange(-1, end, &sortedset.GetByScoreRangeOptions{
			Limit:  limit,
			Remove: true})
	}
	b.mu.Unlock()

	response := make([]*StorageItem, 0, len(nodes))

	//TODO there is obvious performace improvement opportunity in a bulk transaction.
	//but need to weigh the risk of the transaction errors by growing too large.
	//https://dgraph.io/docs/badger/get-started/#read-write-transactions (check example here)
	for i := range nodes {
		item, err := b.RemoveFromDB(nodes[i].Key())

		if err != nil {
			return nil, err
		}
		response = append(response, item)
	}

	return response, nil
}

//RemoveFromDB reads and deletes by key from badger in a read-write transaction
func (b *BufferCompactor) RemoveFromDB(key string) (*StorageItem, error) {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	item, err := txn.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	if err := txn.Delete([]byte(key)); err != nil {
		return nil, err
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	var value []byte
	value, err = item.ValueCopy(value)
	if err != nil {
		return nil, err
	}

	return &StorageItem{
		Key:   key,
		Value: value,
	}, nil
}

//RetrieveAllKeys is a helper that can be used on startup if using badger with persistant disk options.
//On startup the SortedSet will be empty since it will not persist to the disk; if persiting badger, you may
//    use this function to drive iteration in order to reseed the SortedSet OR
//    process manually and pop them from the store using `RemoveFromDB`
//
//Note: This is a helper function and can be implemented on the badger db instance itself and is not pivotal to this library.
func (b *BufferCompactor) RetrieveAllKeys() ([]string, error) {
	var keys []string
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				keys = append(keys, string(k))
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return keys, nil
}
