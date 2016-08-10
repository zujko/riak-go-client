package riak

import (
	"errors"
	"reflect"
	"testing"
	"time"

	rpbRiakDT "github.com/basho/riak-go-client/rpb/riak_dt"
)

func BenchmarkFetchMapResponse(b *testing.B) {
	cmdImpl := commandImpl{errors.New("error"), true, "This is just a test message. ABC 123 XYZ"}
	toutImpl := timeoutImpl{time.Duration(10000)}
	mapResp := FetchMapResponse{IsNotFound: true, Context: []byte{'a', 'b', 'c'}}
	for n := 0; n < b.N; n++ {
		cmd := &FetchMapCommand{
			cmdImpl,
			toutImpl,
			retryableCommandImpl{},
			&mapResp,
			&rpbRiakDT.DtFetchReq{},
		}
		if got, want := cmd.getRequestCode(), rpbCode_DtFetchReq; got != want {
			b.Errorf("got %v, want %v", got, want)
		}
		if got, want := cmd.getResponseCode(), rpbCode_DtFetchResp; got != want {
			b.Errorf("got %v, want %v", got, want)
		}
		msg := cmd.getResponseProtobufMessage()
		if _, ok := msg.(*rpbRiakDT.DtFetchResp); !ok {
			b.Errorf("error casting %v to DtFetchResp", reflect.TypeOf(msg))
		}
	}
}
