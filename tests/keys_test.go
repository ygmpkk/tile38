package tests

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestKeys(t *testing.T, mc *mockServer) {
	runStep(t, mc, "BOUNDS", keys_BOUNDS_test)
	runStep(t, mc, "DEL", keys_DEL_test)
	runStep(t, mc, "DROP", keys_DROP_test)
	runStep(t, mc, "RENAME", keys_RENAME_test)
	runStep(t, mc, "RENAMENX", keys_RENAMENX_test)
	runStep(t, mc, "EXPIRE", keys_EXPIRE_test)
	runStep(t, mc, "FSET", keys_FSET_test)
	runStep(t, mc, "GET", keys_GET_test)
	runStep(t, mc, "KEYS", keys_KEYS_test)
	runStep(t, mc, "PERSIST", keys_PERSIST_test)
	runStep(t, mc, "SET", keys_SET_test)
	runStep(t, mc, "STATS", keys_STATS_test)
	runStep(t, mc, "TTL", keys_TTL_test)
	runStep(t, mc, "SET EX", keys_SET_EX_test)
	runStep(t, mc, "PDEL", keys_PDEL_test)
	runStep(t, mc, "FIELDS", keys_FIELDS_test)
	runStep(t, mc, "WHEREIN", keys_WHEREIN_test)
	runStep(t, mc, "WHEREEVAL", keys_WHEREEVAL_test)
	runStep(t, mc, "TYPE", keys_TYPE_test)
}

func keys_BOUNDS_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("BOUNDS", "mykey").Str("<nil>"),
		Do("BOUNDS", "mykey").JSON().Err("key not found"),
		Do("SET", "mykey", "myid1", "POINT", 33, -115).OK(),
		Do("BOUNDS", "mykey").Str("[[-115 33] [-115 33]]"),
		Do("BOUNDS", "mykey").JSON().Str(`{"ok":true,"bounds":{"type":"Point","coordinates":[-115,33]}}`),
		Do("SET", "mykey", "myid2", "POINT", 34, -112).OK(),
		Do("BOUNDS", "mykey").Str("[[-115 33] [-112 34]]"),
		Do("DEL", "mykey", "myid2").Str("1"),
		Do("BOUNDS", "mykey").Str("[[-115 33] [-115 33]]"),
		Do("SET", "mykey", "myid3", "OBJECT", `{"type":"Point","coordinates":[-130,38,10]}`).OK(),
		Do("SET", "mykey", "myid4", "OBJECT", `{"type":"Point","coordinates":[-110,25,-8]}`).OK(),
		Do("BOUNDS", "mykey").Str("[[-130 25] [-110 38]]"),
		Do("BOUNDS", "mykey", "hello").Err("wrong number of arguments for 'bounds' command"),
		Do("BOUNDS", "nada").Str("<nil>"),
		Do("BOUNDS", "nada").JSON().Err("key not found"),
		Do("BOUNDS", "").Str("<nil>"),
		Do("BOUNDS", "mykey").JSON().Str(`{"ok":true,"bounds":{"type":"Polygon","coordinates":[[[-130,25],[-110,25],[-110,38],[-130,38],[-130,25]]]}}`),
	)
}

func keys_DEL_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid", "POINT").Str("[33 -115]"),
		Do("DEL", "mykey", "myid2", "ERRON404").Err("id not found"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("DEL", "mykey", "myid").Str("0"),
		Do("DEL", "mykey").Err("wrong number of arguments for 'del' command"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("DEL", "mykey", "myid", "ERRON404").Err("key not found"),
		Do("DEL", "mykey", "myid", "invalid-arg").Err("invalid argument 'invalid-arg'"),
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("DEL", "mykey", "myid2", "ERRON404").JSON().Err("id not found"),
		Do("DEL", "mykey", "myid").JSON().OK(),
		Do("DEL", "mykey", "myid").JSON().OK(),
		Do("DEL", "mykey", "myid", "ERRON404").JSON().Err("key not found"),
	)
}

func keys_DROP_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid2", "HASH", "9my5xp8").OK(),
		Do("SCAN", "mykey", "COUNT").Str("2"),
		Do("DROP").Err("wrong number of arguments for 'drop' command"),
		Do("DROP", "mykey", "arg3").Err("wrong number of arguments for 'drop' command"),
		Do("DROP", "mykey").Str("1"),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("DROP", "mykey").Str("0"),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("DROP", "mykey").JSON().OK(),
		Do("DROP", "mykey").JSON().OK(),
	)
}
func keys_RENAME_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid2", "HASH", "9my5xp8").OK(),
		Do("SCAN", "mykey", "COUNT").Str("2"),
		Do("RENAME", "foo", "mynewkey", "arg3").Err("wrong number of arguments for 'rename' command"),
		Do("RENAME", "mykey", "mynewkey").OK(),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("SCAN", "mynewkey", "COUNT").Str("2"),
		Do("SET", "mykey", "myid3", "HASH", "9my5xp7").OK(),
		Do("RENAME", "mykey", "mynewkey").OK(),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("SCAN", "mynewkey", "COUNT").Str("1"),
		Do("RENAME", "foo", "mynewkey").Err("key not found"),
		Do("SCAN", "mynewkey", "COUNT").Str("1"),
		Do("SETCHAN", "mychan", "INTERSECTS", "mynewkey", "BOUNDS", 10, 10, 20, 20).Str("1"),
		Do("RENAME", "mynewkey", "foo2").Err("key has hooks set"),
		Do("RENAMENX", "mynewkey", "foo2").Err("key has hooks set"),
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("RENAME", "mykey", "foo2").OK(),
		Do("RENAMENX", "foo2", "foo3").Str("1"),
		Do("RENAMENX", "foo2", "foo3").Err("key not found"),
		Do("RENAME", "foo2", "foo3").JSON().Err("key not found"),
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("RENAMENX", "mykey", "foo3").Str("0"),
		Do("RENAME", "foo3", "foo4").JSON().OK(),
	)
}
func keys_RENAMENX_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid2", "HASH", "9my5xp8").OK(),
		Do("SCAN", "mykey", "COUNT").Str("2"),
		Do("RENAMENX", "mykey", "mynewkey").Str("1"),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("DROP", "mykey").Str("0"),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("SCAN", "mynewkey", "COUNT").Str("2"),
		Do("SET", "mykey", "myid3", "HASH", "9my5xp7").OK(),
		Do("RENAMENX", "mykey", "mynewkey").Str("0"),
		Do("SCAN", "mykey", "COUNT").Str("1"),
		Do("SCAN", "mynewkey", "COUNT").Str("2"),
		Do("RENAMENX", "foo", "mynewkey").Str("ERR key not found"),
		Do("SCAN", "mynewkey", "COUNT").Str("2"),
	)
}
func keys_EXPIRE_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"EXPIRE", "mykey", "myid", 1}, {1},
		{time.Second / 4}, {}, // sleep
		{"GET", "mykey", "myid"}, {"value"},
		{time.Second}, {}, // sleep
		{"GET", "mykey", "myid"}, {nil},
	})
}
func keys_FSET_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "HASH", "9my5xp7"}, {"OK"},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7]"},
		{"FSET", "mykey", "myid", "f1", 105.6}, {1},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [f1 105.6]]"},
		{"FSET", "mykey", "myid", "f1", 1.1, "f2", 2.2}, {2},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [f1 1.1 f2 2.2]]"},
		{"FSET", "mykey", "myid", "f1", 1.1, "f2", 22.22}, {1},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [f1 1.1 f2 22.22]]"},
		{"FSET", "mykey", "myid", "f1", 0}, {1},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [f2 22.22]]"},
		{"FSET", "mykey", "myid", "f2", 0}, {1},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7]"},
		{"FSET", "mykey", "myid2", "xx", "f1", 1.1, "f2", 2.2}, {0},
		{"GET", "mykey", "myid2"}, {nil},
		{"DEL", "mykey", "myid"}, {"1"},
		{"GET", "mykey", "myid"}, {nil},
	})
}
func keys_GET_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "STRING", "value").OK(),
		Do("GET", "mykey", "myid").Str("value"),
		Do("SET", "mykey", "myid", "STRING", "value2").OK(),
		Do("GET", "mykey", "myid").Str("value2"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("GET", "mykey").Err("wrong number of arguments for 'get' command"),
		Do("GET", "mykey", "myid", "hash").Err("wrong number of arguments for 'get' command"),
		Do("GET", "mykey", "myid", "hash", "0").Err("invalid argument '0'"),
		Do("GET", "mykey", "myid", "hash", "-1").Err("invalid argument '-1'"),
		Do("GET", "mykey", "myid", "hash", "13").Err("invalid argument '13'"),
		Do("SET", "mykey", "myid", "field", "hello", "world", "field", "hiya", 55, "point", 33, -112).OK(),
		Do("GET", "mykey", "myid", "hash", "1").Str("9"),
		Do("GET", "mykey", "myid", "point").Str("[33 -112]"),
		Do("GET", "mykey", "myid", "bounds").Str("[[33 -112] [33 -112]]"),
		Do("GET", "mykey", "myid", "object").Str(`{"type":"Point","coordinates":[-112,33]}`),
		Do("GET", "mykey", "myid", "object").Str(`{"type":"Point","coordinates":[-112,33]}`),
		Do("GET", "mykey", "myid", "withfields", "point").Str(`[[33 -112] [hello world hiya 55]]`),
		Do("GET", "mykey", "myid", "joint").Err("wrong number of arguments for 'get' command"),
		Do("GET", "mykey2", "myid").Str("<nil>"),
		Do("GET", "mykey2", "myid").JSON().Err("key not found"),
		Do("GET", "mykey", "myid2").Str("<nil>"),
		Do("GET", "mykey", "myid2").JSON().Err("id not found"),
		Do("GET", "mykey", "myid", "point").JSON().Str(`{"ok":true,"point":{"lat":33,"lon":-112}}`),
		Do("GET", "mykey", "myid", "object").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[-112,33]}}`),
		Do("GET", "mykey", "myid", "hash", "1").JSON().Str(`{"ok":true,"hash":"9"}`),
		Do("GET", "mykey", "myid", "bounds").JSON().Str(`{"ok":true,"bounds":{"sw":{"lat":33,"lon":-112},"ne":{"lat":33,"lon":-112}}}`),
		Do("SET", "mykey", "myid2", "point", 33, -112, 55).OK(),
		Do("GET", "mykey", "myid2", "point").Str("[33 -112 55]"),
		Do("GET", "mykey", "myid2", "point").JSON().Str(`{"ok":true,"point":{"lat":33,"lon":-112,"z":55}}`),
		Do("GET", "mykey", "myid", "withfields").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[-112,33]},"fields":{"hello":"world","hiya":55}}`),
	)
}
func keys_KEYS_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey11", "myid4", "STRING", "value"}, {"OK"},
		{"SET", "mykey22", "myid2", "HASH", "9my5xp7"}, {"OK"},
		{"SET", "mykey22", "myid1", "OBJECT", `{"type":"Point","coordinates":[-130,38,10]}`}, {"OK"},
		{"SET", "mykey11", "myid3", "OBJECT", `{"type":"Point","coordinates":[-110,25,-8]}`}, {"OK"},
		{"SET", "mykey42", "myid2", "HASH", "9my5xp7"}, {"OK"},
		{"SET", "mykey31", "myid4", "STRING", "value"}, {"OK"},
		{"SET", "mykey310", "myid5", "STRING", "value"}, {"OK"},
		{"KEYS", "*"}, {"[mykey11 mykey22 mykey31 mykey310 mykey42]"},
		{"KEYS", "*key*"}, {"[mykey11 mykey22 mykey31 mykey310 mykey42]"},
		{"KEYS", "mykey*"}, {"[mykey11 mykey22 mykey31 mykey310 mykey42]"},
		{"KEYS", "mykey4*"}, {"[mykey42]"},
		{"KEYS", "mykey*1"}, {"[mykey11 mykey31]"},
		{"KEYS", "mykey*1*"}, {"[mykey11 mykey31 mykey310]"},
		{"KEYS", "mykey*10"}, {"[mykey310]"},
		{"KEYS", "mykey*2"}, {"[mykey22 mykey42]"},
		{"KEYS", "*2"}, {"[mykey22 mykey42]"},
		{"KEYS", "*1*"}, {"[mykey11 mykey31 mykey310]"},
		{"KEYS", "mykey"}, {"[]"},
		{"KEYS", "mykey31"}, {"[mykey31]"},
		{"KEYS", "mykey[^3]*"}, {"[mykey11 mykey22 mykey42]"},
	})
}
func keys_PERSIST_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"EXPIRE", "mykey", "myid", 2}, {1},
		{"PERSIST", "mykey", "myid"}, {1},
		{"PERSIST", "mykey", "myid"}, {0},
	})
}
func keys_SET_test(mc *mockServer) error {
	return mc.DoBatch(
		"point", [][]interface{}{
			{"SET", "mykey", "myid", "POINT", 33, -115}, {"OK"},
			{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
			{"GET", "mykey", "myid", "BOUNDS"}, {"[[33 -115] [33 -115]]"},
			{"GET", "mykey", "myid", "OBJECT"}, {`{"type":"Point","coordinates":[-115,33]}`},
			{"GET", "mykey", "myid", "HASH", 7}, {"9my5xp7"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"object", [][]interface{}{
			{"SET", "mykey", "myid", "OBJECT", `{"type":"Point","coordinates":[-115,33]}`}, {"OK"},
			{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
			{"GET", "mykey", "myid", "BOUNDS"}, {"[[33 -115] [33 -115]]"},
			{"GET", "mykey", "myid", "OBJECT"}, {`{"type":"Point","coordinates":[-115,33]}`},
			{"GET", "mykey", "myid", "HASH", 7}, {"9my5xp7"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"bounds", [][]interface{}{
			{"SET", "mykey", "myid", "BOUNDS", 33, -115, 33, -115}, {"OK"},
			{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
			{"GET", "mykey", "myid", "BOUNDS"}, {"[[33 -115] [33 -115]]"},
			{"GET", "mykey", "myid", "OBJECT"}, {`{"type":"Point","coordinates":[-115,33]}`},
			{"GET", "mykey", "myid", "HASH", 7}, {"9my5xp7"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"hash", [][]interface{}{
			{"SET", "mykey", "myid", "HASH", "9my5xp7"}, {"OK"},
			{"GET", "mykey", "myid", "HASH", 7}, {"9my5xp7"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"field", [][]interface{}{
			{"SET", "mykey", "myid", "FIELD", "f1", 33, "FIELD", "a2", 44.5, "HASH", "9my5xp7"}, {"OK"},
			{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [a2 44.5 f1 33]]"},
			{"FSET", "mykey", "myid", "f1", 0}, {1},
			{"FSET", "mykey", "myid", "f1", 0}, {0},
			{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [a2 44.5]]"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"string", [][]interface{}{
			{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
			{"GET", "mykey", "myid"}, {"value"},
			{"SET", "mykey", "myid", "STRING", "value2"}, {"OK"},
			{"GET", "mykey", "myid"}, {"value2"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
	)
}

func keys_STATS_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"STATS", "mykey"}, {"[nil]"},
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"STATS", "mykey"}, {"[[in_memory_size 9 num_objects 1 num_points 0 num_strings 1]]"},
		{"SET", "mykey", "myid2", "STRING", "value"}, {"OK"},
		{"STATS", "mykey"}, {"[[in_memory_size 19 num_objects 2 num_points 0 num_strings 2]]"},
		{"SET", "mykey", "myid3", "OBJECT", `{"type":"Point","coordinates":[-115,33]}`}, {"OK"},
		{"STATS", "mykey"}, {"[[in_memory_size 40 num_objects 3 num_points 1 num_strings 2]]"},
		{"DEL", "mykey", "myid"}, {1},
		{"STATS", "mykey"}, {"[[in_memory_size 31 num_objects 2 num_points 1 num_strings 1]]"},
		{"DEL", "mykey", "myid3"}, {1},
		{"STATS", "mykey"}, {"[[in_memory_size 10 num_objects 1 num_points 0 num_strings 1]]"},
		{"STATS", "mykey", "mykey2"}, {"[[in_memory_size 10 num_objects 1 num_points 0 num_strings 1] nil]"},
		{"DEL", "mykey", "myid2"}, {1},
		{"STATS", "mykey"}, {"[nil]"},
		{"STATS", "mykey", "mykey2"}, {"[nil nil]"},
	})
}
func keys_TTL_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"EXPIRE", "mykey", "myid", 2}, {1},
		{time.Second / 4}, {}, // sleep
		{"TTL", "mykey", "myid"}, {1},
	})
}

func keys_SET_EX_test(mc *mockServer) (err error) {
	rand.Seed(time.Now().UnixNano())

	// add a bunch of points
	for i := 0; i < 20000; i++ {
		val := fmt.Sprintf("val:%d", i)
		var resp string
		var lat, lon float64
		lat = rand.Float64()*180 - 90
		lon = rand.Float64()*360 - 180
		resp, err = redis.String(mc.conn.Do("SET",
			fmt.Sprintf("mykey%d", i%3), val,
			"EX", 1+rand.Float64(),
			"POINT", lat, lon))
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
	mc.conn.Do("OUTPUT", "json")
	json, _ := redis.String(mc.conn.Do("SERVER"))
	if !gjson.Get(json, "ok").Bool() {
		return errors.New("not ok")
	}
	if gjson.Get(json, "stats.num_objects").Int() > 0 {
		return errors.New("items left in database")
	}
	mc.conn.Do("FLUSHDB")
	return nil
}

func keys_FIELDS_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid1a", "FIELD", "a", 1, "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1]]`},
		{"SET", "mykey", "myid1a", "FIELD", "a", "a", "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a a]]`},
		{"SET", "mykey", "myid1a", "FIELD", "a", 1, "FIELD", "b", 2, "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2]]`},
		{"SET", "mykey", "myid1a", "FIELD", "b", 2, "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2]]`},
		{"SET", "mykey", "myid1a", "FIELD", "b", 2, "FIELD", "a", "1", "FIELD", "c", 3, "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2 c 3]]`},
	})
}

func keys_PDEL_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid1b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid2a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid2b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid3a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid3b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid4a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid4b", "POINT", 33, -115).OK(),
		Do("PDEL", "mykey").Err("wrong number of arguments for 'pdel' command"),
		Do("PDEL", "mykeyNA", "*").Str("0"),
		Do("PDEL", "mykey", "myid1a").Str("1"),
		Do("PDEL", "mykey", "myid1a").Str("0"),
		Do("PDEL", "mykey", "myid1*").Str("1"),
		Do("PDEL", "mykey", "myid2*").Str("2"),
		Do("PDEL", "mykey", "*b").Str("2"),
		Do("PDEL", "mykey", "*").Str("2"),
		Do("PDEL", "mykey", "*").Str("0"),
		Do("SET", "mykey", "myid1a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid1b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid2a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid2b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid3a", "POINT", 33, -115).OK(),
		Do("PDEL", "mykey", "*").JSON().OK(),
	)
}

func keys_WHEREIN_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid_a1", "FIELD", "a", 1, "POINT", 33, -115}, {"OK"},
		{"WITHIN", "mykey", "WHEREIN", "a", 3, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
		{"WITHIN", "mykey", "WHEREIN", "a", "a", 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument 'a'"},
		{"WITHIN", "mykey", "WHEREIN", "a", 1, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument '1'"},
		{"WITHIN", "mykey", "WHEREIN", "a", 3, 0, "a", 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"[0 []]"},
		{"WITHIN", "mykey", "WHEREIN", "a", 4, 0, "a", 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
		{"SET", "mykey", "myid_a2", "FIELD", "a", 2, "POINT", 32.99, -115}, {"OK"},
		{"SET", "mykey", "myid_a3", "FIELD", "a", 3, "POINT", 33, -115.02}, {"OK"},
		{"WITHIN", "mykey", "WHEREIN", "a", 3, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {
			`[0 [[myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]] [myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
		// zero value should not match 1 and 2
		{"SET", "mykey", "myid_a0", "FIELD", "a", 0, "POINT", 33, -115.02}, {"OK"},
		{"WITHIN", "mykey", "WHEREIN", "a", 2, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]] [myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
	})
}

func keys_WHEREEVAL_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid_a1", "FIELD", "a", 1, "POINT", 33, -115}, {"OK"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", 1, 0.5, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", "a", 0.5, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument 'a'"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", 1, 0.5, 4, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument '4'"},
		{"SET", "mykey", "myid_a2", "FIELD", "a", 2, "POINT", 32.99, -115}, {"OK"},
		{"SET", "mykey", "myid_a3", "FIELD", "a", 3, "POINT", 33, -115.02}, {"OK"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1]) and FIELDS.a ~= tonumber(ARGV[2])", 2, 0.5, 3, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]] [myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
	})
}

func keys_TYPE_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1", "POINT", 33, -115).OK(),
		Do("TYPE", "mykey").Str("hash"),
		Do("TYPE", "mykey", "hello").Err("wrong number of arguments for 'type' command"),
		Do("TYPE", "mykey2").Str("none"),
		Do("TYPE", "mykey2").JSON().Err("key not found"),
		Do("TYPE", "mykey").JSON().Str(`{"ok":true,"type":"hash"}`),
	)
}
