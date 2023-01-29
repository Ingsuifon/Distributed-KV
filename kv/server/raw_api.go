package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		response.Error = "Failed to get StorageReader."
		return response, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	response.NotFound = val == nil;
	response.Value = val
	return response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := new(kvrpcpb.RawPutResponse)
	record := storage.Modify {Data: storage.Put {Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}}
	err := server.storage.Write(req.GetContext(), []storage.Modify{record})
	if err != nil {
		response.Error = "Failed to put!"
		return response, err
	}
	return response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := new(kvrpcpb.RawDeleteResponse)
	record := storage.Modify {Data: storage.Delete {Key: req.GetKey(), Cf: req.GetCf()}}
	err := server.storage.Write(req.GetContext(), []storage.Modify{record})
	if err != nil {
		response.Error = "Failed to delete."
		return response, err
	}
	return response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		response.Error = "Failed to get StorageReader."
		return response, err
	}
	iter := reader.IterCF(req.GetCf())
	var num uint32 = 0
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if num == req.GetLimit() {
			break
		}
		item := iter.Item()
		v, err := item.Value()
		if err != nil {
			response.Error = "Scan failure."
			iter.Close()
			return response, err
		}
		pair := &kvrpcpb.KvPair {Key: item.Key(), Value: v}
		response.Kvs = append(response.Kvs, pair)
		num++
	}
	iter.Close()
	return response, nil
}
