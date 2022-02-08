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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/parkerroan/buffercompact/sortedset"
)

var (
	ErrMaxValueCount = errors.New("max value count reached")
	DedupeKeyPrefix  = "unique_value:%s"
	KeyPrefix        = "compact_key:%s"
)

type BufferCompactor struct {
	db             *badger.DB
	sortedSet      *sortedset.SortedSet
	bufferDuration time.Duration
	mu             sync.Mutex

	maxValuesCount int
	ttlDuration    *time.Duration
	dedupeDuration *time.Duration
}

type BufferCompactorOption func(*BufferCompactor)

type StorageItem struct {
	Key      string
	Value    []byte
	UniqueID string

	score int64
}

func New(db *badger.DB, bufferDuration time.Duration, opts ...BufferCompactorOption) (*BufferCompactor, error) {
	buffComp := BufferCompactor{
		db:             db,
		sortedSet:      sortedset.New(),
		bufferDuration: bufferDuration,
	}

	for _, opt := range opts {
		opt(&buffComp)
	}

	if err := buffComp.PopulateSetFromDB(); err != nil {
		return nil, err
	}

	return &buffComp, nil
}

func WithMaxValueCount(maxLength int) BufferCompactorOption {
	return func(b *BufferCompactor) {
		b.maxValuesCount = maxLength
	}
}

func WithSortedSet(set *sortedset.SortedSet) BufferCompactorOption {
	return func(b *BufferCompactor) {
		b.sortedSet = set
	}
}

func WithTTL(ttlDuration time.Duration) BufferCompactorOption {
	return func(b *BufferCompactor) {
		b.ttlDuration = &ttlDuration
	}
}

func WithDedupeDuration(ttlDuration time.Duration) BufferCompactorOption {
	return func(b *BufferCompactor) {
		b.ttlDuration = &ttlDuration
	}
}

func (b *BufferCompactor) StoreToQueue(item StorageItem) error {
	if b.maxValuesCount != 0 && b.sortedSet.GetCount() >= b.maxValuesCount {
		return ErrMaxValueCount
	}

	score := time.Now().Add(b.bufferDuration).Unix()
	item.score = score

	err := b.db.Update(func(txn *badger.Txn) error {
		//Dedupe Block
		if item.UniqueID != "" {
			dedupeKey := []byte(fmt.Sprintf(DedupeKeyPrefix, item.Key))
			if existingItem, _ := txn.Get(dedupeKey); existingItem != nil {
				var existingUniqueIDbytes []byte
				existingUniqueIDbytes, _ = existingItem.ValueCopy(existingUniqueIDbytes)
				if string(existingUniqueIDbytes) == item.UniqueID {
					//value match skipping store for dedupe
					return nil
				}
			}

			dupeEntry := badger.NewEntry(dedupeKey, []byte(item.UniqueID))
			if b.dedupeDuration != nil {
				dupeEntry.WithTTL(*b.dedupeDuration)
			}
			if err := txn.SetEntry(dupeEntry); err != nil {
				return err
			}
		}

		value := appendScoreBytes(item.Value, item.score)
		entry := badger.NewEntry([]byte(item.Key), value)
		if b.ttlDuration != nil {
			entry.WithTTL(*b.ttlDuration)
		}
		err := txn.SetEntry(entry)
		return err
	})
	if err != nil {
		return err
	}

	if node := b.sortedSet.GetByKey(item.Key); node == nil {
		b.sortedSet.AddOrUpdate(item.Key, sortedset.SCORE(score), struct{}{})
	}
	return nil
}

func (b *BufferCompactor) RetrieveFromQueue(limit int) ([]*StorageItem, error) {
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

	score, strippedValue := removeScoreBytes(value)

	return &StorageItem{
		Key:   key,
		Value: strippedValue,
		score: score,
	}, nil
}

//PopulateSetFromDB allows for badgerDB persistance by loading all keys and score from db on startup
//    into the sortedset.
func (b *BufferCompactor) PopulateSetFromDB() error {
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				score, _ := removeScoreBytes(v)
				if node := b.sortedSet.GetByKey(string(k)); node == nil {
					b.sortedSet.AddOrUpdate(string(k), sortedset.SCORE(score), struct{}{})
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func appendScoreBytes(input []byte, score int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(score))

	final := append(input, b...)

	return final
}

func removeScoreBytes(value []byte) (int64, []byte) {
	scoreBytes := value[len(value)-8:]
	value = value[:len(value)-8]
	score := int64(binary.LittleEndian.Uint64(scoreBytes))

	return score, value
}
