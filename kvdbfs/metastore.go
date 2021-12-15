package kvdbfs

import (
	"github.com/xujiajun/nutsdb"
)

type MetaStore interface {
	Put(key string, value []byte) error
	Delete(key string) error
	Get(key string) (value []byte, err error)
}

var _ MetaStore = (*nutsdbStore)(nil)

type nutsdbStore struct {
	db     *nutsdb.DB
	prefix string
}

func NewNutsdbStore(config *Config) (MetaStore, error) {
	opt := nutsdb.DefaultOptions
	opt.Dir = config.MetaStore.Dir
	opt.SegmentSize = config.MetaStore.SegmentSize
	db, err := nutsdb.Open(opt)
	if err != nil {
		return nil, err
	}
	// db.View(func(tx *nutsdb.Tx) error {
	// 	ens, err := tx.GetAll(config.MetaStore.Prefix)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	for _, entry := range ens {
	// 		fmt.Println(string(entry.Key), string(entry.Value))
	// 	}
	// 	return nil
	// })
	return &nutsdbStore{
		db:     db,
		prefix: config.MetaStore.Prefix,
	}, nil
}

func (ms *nutsdbStore) Put(key string, value []byte) error {
	return ms.db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.Put(ms.prefix, []byte(key), value, 0)
		},
	)
}

func (ms *nutsdbStore) Delete(key string) error {
	return ms.db.Update(
		func(tx *nutsdb.Tx) error {
			return tx.Delete(ms.prefix, []byte(key))
		},
	)
}

func (ms *nutsdbStore) Get(key string) ([]byte, error) {
	var v []byte
	if err := ms.db.View(
		func(tx *nutsdb.Tx) error {
			e, err := tx.Get(ms.prefix, []byte(key))
			if err != nil {
				return err
			}
			logger.Debugf("key: %s; value: %v", key, e.Value)
			v = e.Value
			return nil
		},
	); err != nil {
		return nil, err
	}
	return v, nil
}
