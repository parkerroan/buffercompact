package buffercompact

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/parkerroan/buffercompact/sortedset"
	"github.com/stretchr/testify/assert"
)

func Test_New(t *testing.T) {
	db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	bufferDuration := 5 * time.Second

	buffcomp, err := New(db, bufferDuration)

	assert.Nil(t, err)
	assert.NotNil(t, buffcomp)
}

func Test_NormalUsageCase(t *testing.T) {
	db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	bufferDuration := 1 * time.Second

	buffcomp, err := New(db, bufferDuration)

	assert.Nil(t, err)
	assert.NotNil(t, buffcomp)

	buffcomp.StoreToQueue(StorageItem{Key: "test1", Value: []byte("testValue1")})
	buffcomp.StoreToQueue(StorageItem{Key: "test1", Value: []byte("testValue2")})
	buffcomp.StoreToQueue(StorageItem{Key: "test3", Value: []byte("testValue3")})

	//No Wait
	items, err := buffcomp.RetrieveFromQueue(10)
	assert.Nil(t, err)
	assert.Len(t, items, 0)

	//items should compact and only receive 2
	time.Sleep(2 * time.Second)
	items, err = buffcomp.RetrieveFromQueue(10)
	assert.Nil(t, err)
	assert.Len(t, items, 2)

	assert.Equal(t, "test1", string(items[0].Key))
	assert.Equal(t, "testValue2", string(items[0].Value))
	assert.Equal(t, "test2", string(items[1].Key))
	assert.Equal(t, "testValue3", string(items[1].Value))
}

func Test_MaxValuesCountCase(t *testing.T) {
	db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	bufferDuration := 1 * time.Second

	buffcomp, err := New(db, bufferDuration, WithMaxValueCount(2))

	assert.Nil(t, err)
	assert.NotNil(t, buffcomp)

	buffcomp.StoreToQueue(StorageItem{Key: "test1", Value: []byte("testValue1")})
	buffcomp.StoreToQueue(StorageItem{Key: "test2", Value: []byte("testValue2")})
	err = buffcomp.StoreToQueue(StorageItem{Key: "test3", Value: []byte("testValue3")})
	assert.EqualError(t, err, ErrMaxValueCount.Error())

	//No Wait but MaxValue empties memory db values
	items, err := buffcomp.RetrieveFromQueue(10)
	assert.Nil(t, err)
	assert.Len(t, items, 2)

	assert.Equal(t, StorageItem{Key: "test1", Value: []byte("testValue1")}, items[0])
	assert.Equal(t, StorageItem{Key: "test2", Value: []byte("testValue2")}, items[1])
}

func Test_PopulateSetFromDB(t *testing.T) {
	db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	bufferDuration := 2 * time.Second

	buffcomp, err := New(db, bufferDuration, WithMaxValueCount(5))

	assert.Nil(t, err)
	assert.NotNil(t, buffcomp)

	buffcomp.StoreToQueue(StorageItem{Key: "test1", Value: []byte("testValue1")})
	buffcomp.StoreToQueue(StorageItem{Key: "test2", Value: []byte("testValue2")})
	buffcomp.StoreToQueue(StorageItem{Key: "test3", Value: []byte("testValue3")})

	//Create new buffer compactor to replicate loading with a populated badgerdb
	buffcomp2, err := New(db, bufferDuration, WithMaxValueCount(5))

	//Wait the previous buffer duration
	time.Sleep(2 * time.Second)
	items, err := buffcomp2.RetrieveFromQueue(10)
	assert.Nil(t, err)
	assert.Len(t, items, 3)
}

func Test_StoreToQueue(test *testing.T) {
	cases := map[string]struct {
		item        StorageItem
		expectedErr error
	}{
		"Happy Path": {
			item: StorageItem{
				Key:   "test1",
				Value: []byte("test_value1"),
			},
		},
	}

	for name, c := range cases {
		test.Run(name, func(t *testing.T) {
			sortedset := sortedset.New()
			db, _ := badger.Open(badger.DefaultOptions("").WithInMemory(true))
			bufferDuration := 5 * time.Second

			buffcomp, err := New(db, bufferDuration)
			assert.Nil(t, err)

			err = buffcomp.StoreToQueue(c.item)
			if c.expectedErr == nil {
				assert.Nil(t, err)
				node := sortedset.GetByKey(c.item.Key)
				assert.Equal(t, c.item.Key, node.Key())

				if err := db.View(func(txn *badger.Txn) error {
					dbItem, err := txn.Get([]byte(c.item.Key))
					if err != nil {
						t.Fatal("error in bagder txn")
					}

					var dbValue []byte
					dbValue, err = dbItem.ValueCopy(dbValue)

					assert.Equal(t, c.item.Value, dbValue)
					return err
				}); err != nil {
					t.Fatal("error in bagder txn")
				}

			} else {
				assert.EqualError(t, err, c.expectedErr.Error())
			}
		})
	}
}

func Test_appendScoreBytes_removeScoreBytes(t *testing.T) {
	value := []byte("test-value")
	score := int64(time.Now().Unix())

	compiledBytes := appendScoreBytes(value, score)
	assert.NotEqual(t, value, compiledBytes)

	actualScore, actualValue := removeScoreBytes(compiledBytes)
	assert.Equal(t, value, actualValue)
	assert.Equal(t, score, actualScore)
}
