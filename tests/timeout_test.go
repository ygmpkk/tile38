package tests

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func subTestTimeout(t *testing.T, mc *mockServer) {
	runStep(t, mc, "session set/unset", timeout_session_set_unset_test)
	runStep(t, mc, "session spatial", timeout_session_spatial_test)
	runStep(t, mc, "session search", timeout_session_search_test)
	runStep(t, mc, "session scripts", timeout_session_scripts_test)
	runStep(t, mc, "command spatial", timeout_command_spatial_test)
	runStep(t, mc, "command search", timeout_command_search_test)
}

func setup(mc *mockServer, count int, points bool) (err error) {
	rand.Seed(time.Now().UnixNano())

	// add a bunch of points
	for i := 0; i < count; i++ {
		val := fmt.Sprintf("val:%d", i)
		var resp string
		var lat, lon, fval float64
		fval = rand.Float64()
		if points {
			lat = rand.Float64()*180 - 90
			lon = rand.Float64()*360 - 180
			resp, err = redis.String(mc.conn.Do("SET",
				"mykey", val,
				"FIELD", "foo", fval,
				"POINT", lat, lon))
		} else {
			resp, err = redis.String(mc.conn.Do("SET",
				"mykey", val,
				"FIELD", "foo", fval,
				"STRING", val))
		}
		if err != nil {
			return
		}
		if resp != "OK" {
			err = fmt.Errorf("expected 'OK', got '%s'", resp)
			return
		}
		time.Sleep(time.Nanosecond)
	}
	time.Sleep(time.Second * 3)
	return
}

func timeout_session_set_unset_test(mc *mockServer) (err error) {
	return mc.DoBatch([][]interface{}{
		{"TIMEOUT"}, {"0"},
		{"TIMEOUT", "0.25"}, {"OK"},
		{"TIMEOUT"}, {"0.25"},
		{"TIMEOUT", "0"}, {"OK"},
		{"TIMEOUT"}, {"0"},
	})
}

func timeout_session_spatial_test(mc *mockServer) (err error) {
	err = setup(mc, 10000, true)

	return mc.DoBatch([][]interface{}{
		{"SCAN", "mykey", "WHERE", "foo", -1, 2, "COUNT"}, {"10000"},
		{"INTERSECTS", "mykey", "WHERE", "foo", -1, 2, "COUNT", "BOUNDS", -90, -180, 90, 180}, {"10000"},
		{"WITHIN", "mykey", "WHERE", "foo", -1, 2, "COUNT", "BOUNDS", -90, -180, 90, 180}, {"10000"},

		{"TIMEOUT", "0.000001"}, {"OK"},

		{"SCAN", "mykey", "WHERE", "foo", -1, 2, "COUNT"}, {"ERR timeout"},
		{"INTERSECTS", "mykey", "WHERE", "foo", -1, 2, "COUNT", "BOUNDS", -90, -180, 90, 180}, {"ERR timeout"},
		{"WITHIN", "mykey", "WHERE", "foo", -1, 2, "COUNT", "BOUNDS", -90, -180, 90, 180}, {"ERR timeout"},
	})
}

func timeout_command_spatial_test(mc *mockServer) (err error) {
	err = setup(mc, 10000, true)

	return mc.DoBatch([][]interface{}{
		{"TIMEOUT", "1"}, {"OK"},
		{"SCAN", "mykey", "WHERE", "foo", -1, 2, "COUNT"}, {"10000"},
		{"INTERSECTS", "mykey", "WHERE", "foo", -1, 2, "COUNT", "BOUNDS", -90, -180, 90, 180}, {"10000"},
		{"WITHIN", "mykey", "WHERE", "foo", -1, 2, "COUNT", "BOUNDS", -90, -180, 90, 180}, {"10000"},

		{"SCAN", "mykey", "TIMEOUT", "0.000001", "WHERE", "foo", -1, 2, "COUNT"}, {"ERR timeout"},
		{"INTERSECTS", "mykey", "TIMEOUT", "0.000001", "WHERE", "foo", -1, 2, "COUNT", "BOUNDS", -90, -180, 90, 180}, {"ERR timeout"},
		{"WITHIN", "mykey", "TIMEOUT", "0.000001", "WHERE", "foo", -1, 2, "COUNT", "BOUNDS", -90, -180, 90, 180}, {"ERR timeout"},
	})
}

func timeout_session_search_test(mc *mockServer) (err error) {
	err = setup(mc, 10000, false)

	return mc.DoBatch([][]interface{}{
		{"SEARCH", "mykey", "MATCH", "val:*", "COUNT"}, {"10000"},
		{"TIMEOUT", "0.000001"}, {"OK"},
		{"SEARCH", "mykey", "MATCH", "val:*", "COUNT"}, {"ERR timeout"},
	})
}

func timeout_command_search_test(mc *mockServer) (err error) {
	err = setup(mc, 10000, false)

	return mc.DoBatch([][]interface{}{
		{"TIMEOUT", "1"}, {"OK"},
		{"SEARCH", "mykey", "MATCH", "val:*", "COUNT"}, {"10000"},
		{"SEARCH", "mykey", "TIMEOUT", "0.000001", "MATCH", "val:*", "COUNT"}, {"ERR timeout"},
	})
}

func timeout_session_scripts_test(mc *mockServer) (err error) {
	script := `
		local clock = os.clock
		local function sleep(n)
			local t0 = clock()
			while clock() - t0 <= n do end
		end
		sleep(0.5)
	`
	sha := "e3ce9449853a622327f30c727a6e086ccd91d9d4"

	return mc.DoBatch([][]interface{}{
		{"SCRIPT LOAD", script}, {sha},

		{"EVALSHA", sha, 0}, {nil},
		{"EVALROSHA", sha, 0}, {nil},
		{"EVALNASHA", sha, 0}, {nil},

		{"TIMEOUT", "0.1"}, {"OK"},

		{"EVALSHA", sha, 0}, {"ERR timeout"},
		{"EVALROSHA", sha, 0}, {"ERR timeout"},
		{"EVALNASHA", sha, 0}, {"ERR timeout"},

		{"TIMEOUT", "0.9"}, {"OK"},

		{"EVALSHA", sha, 0}, {nil},
		{"EVALROSHA", sha, 0}, {nil},
		{"EVALNASHA", sha, 0}, {nil},
	})
}
