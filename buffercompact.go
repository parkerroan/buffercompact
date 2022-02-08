package buffercompact

import (
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/parkerroan/buffercompact/sortedset"
)

type BufferCompactor struct {
	db             *badger.DB
	sortedSet      *sortedset.SortedSet
	bufferDuration time.Duration

	maxSetLength *int
}

type StorageItem struct {
	Key   string
	Value []byte
}

func New(db *badger.DB, sortedSet *sortedset.SortedSet, bufferDuration time.Duration, opts ...func(*BufferCompactor)) (*BufferCompactor, error) {
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

func (b *BufferCompactor) StoreToQueue(key string, value []byte) error {

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

	//if max set length is hit, aggressively remove items disregarding
	//buffer duration
	if b.maxSetLength != nil && b.sortedSet.GetCount() > *b.maxSetLength {
		nodes = b.sortedSet.GetByRankRange(1, limit, true)
	} else {
		end := sortedset.SCORE(time.Now().Unix())
		nodes = b.sortedSet.GetByScoreRange(-1, end, &sortedset.GetByScoreRangeOptions{
			Limit:  limit,
			Remove: true})
	}

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