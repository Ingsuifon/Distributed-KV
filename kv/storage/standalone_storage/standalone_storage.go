package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	storage := new(StandAloneStorage)
	db := engine_util.CreateDB(conf.DBPath, conf.Raft)
	storage.engine = engine_util.NewEngines(db, nil, conf.DBPath, "")
	return storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	_txn := s.engine.Kv.NewTransaction(false)
	return &StandAlongReader{_txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	write_batch := new(engine_util.WriteBatch)
	for _, m := range batch {
		write_batch.SetCF(m.Cf(), m.Key(), m.Value())
	}
	s.engine.WriteKV(write_batch)
	return nil
}

type StandAlongReader struct {
	txn *badger.Txn
}

func (r *StandAlongReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if val == nil {
		err = nil
	}
	return val, err
}

func (r *StandAlongReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAlongReader) Close() {
	r.txn.Commit()
}