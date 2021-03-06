package riak

import (
	"fmt"

	rpb_riak "github.com/basho/riak-go-client/rpb/riak"
	proto "github.com/golang/protobuf/proto"
)

type RiakError struct {
	Errcode uint32
	Errmsg  string
}

func newRiakError(rpb *rpb_riak.RpbErrorResp) (e error) {
	return RiakError{
		Errcode: rpb.GetErrcode(),
		Errmsg:  string(rpb.GetErrmsg()),
	}
}

func maybeRiakError(data []byte) (err error) {
	rpbMsgCode := data[0]
	if rpbMsgCode == rpbCode_RpbErrorResp {
		rpb := &rpb_riak.RpbErrorResp{}
		err = proto.Unmarshal(data[1:], rpb)
		if err == nil {
			// No error in Unmarshal, so construct RiakError
			err = newRiakError(rpb)
		}
	}
	return
}

func (e RiakError) Error() (s string) {
	return fmt.Sprintf("RiakError|%d|%s", e.Errcode, e.Errmsg)
}

// Client errors
var (
	ErrAddressRequired      = newClientError("RemoteAddress is required in options", nil)
	ErrAuthMissingConfig    = newClientError("[Connection] authentication is missing TLS config", nil)
	ErrAuthTLSUpgradeFailed = newClientError("[Connection] upgrading to TLS connection failed", nil)
	ErrBucketRequired       = newClientError("Bucket is required", nil)
	ErrKeyRequired          = newClientError("Key is required", nil)
	ErrNilOptions           = newClientError("[Command] options must be non-nil", nil)
	ErrOptionsRequired      = newClientError("Options are required", nil)
	ErrZeroLength           = newClientError("[Command] 0 byte data response", nil)
	ErrTableRequired        = newClientError("Table is required", nil)
	ErrQueryRequired        = newClientError("Query is required", nil)
)

type ClientError struct {
	Errmsg     string
	InnerError error
}

func newClientError(errmsg string, innerError error) error {
	return ClientError{
		Errmsg:     errmsg,
		InnerError: innerError,
	}
}

func (e ClientError) Error() (s string) {
	if e.InnerError == nil {
		return fmt.Sprintf("ClientError|%s", e.Errmsg)
	}
	return fmt.Sprintf("ClientError|%s|InnerError|%v", e.Errmsg, e.InnerError)
}
