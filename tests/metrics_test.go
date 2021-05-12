package server

import "testing"

func subTestInfo(t *testing.T, mc *mockServer) {
	runStep(t, mc, "valid json", info_valid_json_test)
}

