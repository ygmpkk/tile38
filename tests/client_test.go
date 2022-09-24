package tests

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestClient(t *testing.T, mc *mockServer) {
	runStep(t, mc, "OUTPUT", client_OUTPUT_test)
	runStep(t, mc, "CLIENT", client_CLIENT_test)
}

func client_OUTPUT_test(mc *mockServer) error {
	if err := mc.DoBatch(
		// tests removal of "elapsed" member.
		Do("OUTPUT", "json").Str(`{"ok":true}`),
		Do("OUTPUT", "resp").OK(),
	); err != nil {
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

func client_CLIENT_test(mc *mockServer) error {
	numConns := 20
	var conns []redis.Conn
	defer func() {
		for i := range conns {
			conns[i].Close()
		}
	}()
	for i := 0; i <= numConns; i++ {
		conn, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
		if err != nil {
			return err
		}
		conns = append(conns, conn)
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

	return mc.DoBatch(
		Do("CLIENT", "list").JSON().Func(func(s string) error {
			if int(gjson.Get(s, "list.#").Int()) < numConns {
				return errors.New("Invalid number of connections")
			}
			return nil
		}),
		Do("CLIENT", "list").Func(func(s string) error {
			if len(strings.Split(strings.TrimSpace(s), "\n")) < numConns {
				return errors.New("Invalid number of connections")
			}
			return nil
		}),
		Do("CLIENT").Err(`wrong number of arguments for 'client' command`),
		Do("CLIENT", "hello").Err(`Syntax error, try CLIENT (LIST | KILL | GETNAME | SETNAME)`),
		Do("CLIENT", "list", "arg3").Err(`wrong number of arguments for 'client' command`),
		Do("CLIENT", "getname", "arg3").Err(`wrong number of arguments for 'client' command`),
		Do("CLIENT", "getname").JSON().Str(`{"ok":true,"name":""}`),
		Do("CLIENT", "getname").Str(``),
	)

}
