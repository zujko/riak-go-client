package riak

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	rpbRiakDT "github.com/basho/riak-go-client/rpb/riak_dt"
)

func BenchmarkFetchMapCommands(b *testing.B) {
	for n := 0; n < b.N; n++ {
		buildDtFetchReqCorrectlyViaFetchMapCommandBuilder(b)
		fetchMapParsesDtFetchRespWithoutValueCorrectly(b)
		fetchMapParsesDtFetchRespCorrectly(b)
	}
}

func fetchMapParsesDtFetchRespCorrectly(b *testing.B) {
	dtFetchResp := &rpbRiakDT.DtFetchResp{
		Type:    rpbRiakDT.DtFetchResp_MAP.Enum(),
		Context: crdtContextBytes,
		Value: &rpbRiakDT.DtValue{
			MapValue: createMapValue(),
		},
	}

	builder := NewFetchMapCommandBuilder().
		WithBucketType("sets").
		WithBucket("bucket").
		WithKey("key")
	cmd, err := builder.Build()
	if err != nil {
		b.Fatalf(err.Error())
	}
	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		b.Fatalf(err.Error())
	}
	if protobuf == nil {
		b.Fatalf("protobuf is nil")
	}

	err = cmd.onSuccess(dtFetchResp)
	if err != nil {
		b.Fatalf(err.Error())
	}

	if fc, ok := cmd.(*FetchMapCommand); ok {
		rsp := fc.Response
		if expected, actual := 0, bytes.Compare(crdtContextBytes, rsp.Context); expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
		verifyMp(b, rsp.Map)
		verifyMp(b, rsp.Map.Maps["map_1"])
	} else {
		b.Errorf("ok: %v - could not convert %v to *FetchMapCommand", ok, reflect.TypeOf(cmd))
	}
}

func fetchMapParsesDtFetchRespWithoutValueCorrectly(b *testing.B) {
	builder := NewFetchMapCommandBuilder().
		WithBucketType("maps").
		WithBucket("bucket").
		WithKey("map_1")
	cmd, err := builder.Build()
	if err != nil {
		b.Fatal(err.Error())
	}
	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		b.Fatal(err.Error())
	}
	if protobuf == nil {
		b.Fatal("protobuf is nil")
	}

	dtFetchResp := &rpbRiakDT.DtFetchResp{}
	err = cmd.onSuccess(dtFetchResp)
	if err != nil {
		b.Fatal(err.Error())
	}

	if uc, ok := cmd.(*FetchMapCommand); ok {
		if expected, actual := true, uc.Response.IsNotFound; expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		b.Errorf("ok: %v - could not convert %v to *FetchMapCommand", ok, reflect.TypeOf(cmd))
	}
}

func buildDtFetchReqCorrectlyViaFetchMapCommandBuilder(b *testing.B) {
	builder := NewFetchMapCommandBuilder().
		WithBucketType("maps").
		WithBucket("bucket").
		WithKey("key").
		WithR(1).
		WithPr(2).
		WithNotFoundOk(true).
		WithBasicQuorum(true).
		WithTimeout(time.Second * 20)
	cmd, err := builder.Build()
	if err != nil {
		b.Fatal(err.Error())
	}

	if _, ok := cmd.(retryableCommand); !ok {
		b.Errorf("got %v, want cmd %s to implement retryableCommand", ok, reflect.TypeOf(cmd))
	}

	protobuf, err := cmd.constructPbRequest()
	if err != nil {
		b.Fatal(err.Error())
	}
	if protobuf == nil {
		b.Fatal("protobuf is nil")
	}
	if req, ok := protobuf.(*rpbRiakDT.DtFetchReq); ok {
		if expected, actual := "maps", string(req.GetType()); expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "bucket", string(req.GetBucket()); expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "key", string(req.GetKey()); expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(1), req.GetR(); expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := uint32(2), req.GetPr(); expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetNotfoundOk(); expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := true, req.GetBasicQuorum(); expected != actual {
			b.Errorf("expected %v, got %v", expected, actual)
		}
		validateTout(b, time.Second*20, req.GetTimeout())
	} else {
		b.Errorf("ok: %v - could not convert %v to *rpbRiakDT.DtFetchReq", ok, reflect.TypeOf(protobuf))
	}
}

func validateTout(b *testing.B, e time.Duration, a uint32) {
	actualDuration := time.Duration(a) * time.Millisecond
	if expected, actual := e, actualDuration; expected != actual {
		b.Errorf("expected %v, got %v", expected, actual)
	}
}

func verifyMp(b *testing.B, m *Map) {
	if expected, actual := int64(50), m.Counters["counter_1"]; expected != actual {
		b.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "value_1", string(m.Sets["set_1"][0]); expected != actual {
		b.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "value_2", string(m.Sets["set_1"][1]); expected != actual {
		b.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "1234", string(m.Registers["register_1"]); expected != actual {
		b.Errorf("expected %v, got %v", expected, actual)
	}
	if expected, actual := true, m.Flags["flag_1"]; expected != actual {
		b.Errorf("expected %v, got %v", expected, actual)
	}
}
