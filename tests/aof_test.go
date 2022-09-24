package tests

import (
	"testing"
)

func subTestAOF(t *testing.T, mc *mockServer) {
	runStep(t, mc, "loading", aof_loading_test)
}

func aof_loading_test(mc *mockServer) error {

	// aof, err := mc.readAOF()
	// if err != nil {
	// 	return err
	// }

	// aof = append(aof, "asdfasdf\r\n"...)
	// aof = nil
	// mc2, err := mockOpenServer(MockServerOptions{
	// 	Silent:  false,
	// 	Metrics: false,
	// 	AOFData: aof,
	// })
	// if err != nil {
	// 	return err
	// }
	// defer mc2.Close()

	// time.Sleep(time.Minute)

	// `

	return nil
}
