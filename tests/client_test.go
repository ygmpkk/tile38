package tests

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestClient(t *testing.T, mc *mockServer) {
	runStep(t, mc, "valid json", client_valid_json_test)
	runStep(t, mc, "valid client count", info_valid_client_count_test)
}

func client_valid_json_test(mc *mockServer) error {
	if err := mc.DoBatch([][]interface{}{
		// tests removal of "elapsed" member.
		{"OUTPUT", "json"}, {`{"ok":true}`},
		{"OUTPUT", "resp"}, {`OK`},
	}); err != nil {
		return err
	}
	// run direct commands
	if _, err := mc.Do("OUTPUT", "json"); err != nil {
		return err
	}
	res, err := mc.Do("CLIENT", "list")
	if err != nil {
		return err
	}
	bres, ok := res.([]byte)
	if !ok {
		return errors.New("Failed to type assert CLIENT response")
	}
	sres := string(bres)
	if !gjson.Valid(sres) {
		return errors.New("CLIENT response was invalid")
	}
	info := gjson.Get(sres, "list").String()
	if !gjson.Valid(info) {
		return errors.New("CLIENT.list response was invalid")
	}
	return nil
}

func info_valid_client_count_test(mc *mockServer) error {
	numConns := 20
	var conns []redis.Conn
	for i := 0; i <= numConns; i++ {
		conn, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
		if err != nil {
			return err
		}
		conns = append(conns, conn)
	}
	for i := range conns {
		defer conns[i].Close()
	}
	if _, err := mc.Do("OUTPUT", "JSON"); err != nil {
		return err
	}
	res, err := mc.Do("CLIENT", "list")
	if err != nil {
		return err
	}
	bres, ok := res.([]byte)
	if !ok {
		return errors.New("Failed to type assert CLIENT response")
	}
	sres := string(bres)
	if len(gjson.Get(sres, "list").Array()) < numConns {
		return errors.New("Invalid number of connections")
	}
	return nil
}
