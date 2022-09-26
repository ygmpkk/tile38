package tests

import (
	"bytes"
	"errors"
	"fmt"
)

func subTestAOF(g *testGroup) {
	g.regSubTest("loading", aof_loading_test)
	// g.regSubTest("AOFMD5", aof_AOFMD5_test)
}

func loadAOFAndClose(aof any) error {
	var aofb []byte
	switch aof := aof.(type) {
	case []byte:
		aofb = []byte(aof)
	case string:
		aofb = []byte(aof)
	default:
		return errors.New("aof is not string or bytes")
	}
	mc, err := mockOpenServer(MockServerOptions{
		Silent:  true,
		Metrics: false,
		AOFData: aofb,
	})
	if mc != nil {
		mc.Close()
	}
	return err
}

func aof_loading_test(mc *mockServer) error {

	var err error
	// invalid command
	err = loadAOFAndClose("asdfasdf\r\n")
	if err == nil || err.Error() != "unknown command 'asdfasdf'" {
		return fmt.Errorf("expected '%v', got '%v'",
			"unknown command 'asdfasdf'", err)
	}

	// incomplete command
	err = loadAOFAndClose("set fleet truck point 10 10\r\nasdfasdf")
	if err != nil {
		return err
	}

	// big aof file
	var aof string
	for i := 0; i < 10000; i++ {
		aof += fmt.Sprintf("SET fleet truck%d POINT 10 10\r\n", i)
	}
	err = loadAOFAndClose(aof)
	if err != nil {
		return err
	}

	// extra zeros at various places
	aof = ""
	for i := 0; i < 1000; i++ {
		if i%10 == 0 {
			aof += string(bytes.Repeat([]byte{0}, 100))
		}
		aof += fmt.Sprintf("SET fleet truck%d POINT 10 10\r\n", i)
	}
	aof += string(bytes.Repeat([]byte{0}, 5000))
	err = loadAOFAndClose(aof)
	if err != nil {
		return err
	}

	// bad protocol
	aof = "*2\r\n$1\r\nh\r\n+OK\r\n"
	err = loadAOFAndClose(aof)
	if fmt.Sprintf("%v", err) != "Protocol error: expected '$', got '+'" {
		return fmt.Errorf("expected '%v', got '%v'",
			"Protocol error: expected '$', got '+'", err)
	}

	return nil
}

// func aof_AOFMD5_test(mc *mockServer) error {
// 	return nil
// }
