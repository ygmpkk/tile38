package server

import (
	"strconv"
	"time"

	"github.com/tidwall/resp"
)

func (c *Server) cmdTimeout(msg *Message, client *Client) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var arg string
	var ok bool

	if len(vs) != 0 {
		if _, arg, ok = tokenval(vs); !ok || arg == "" {
			return NOMessage, errInvalidNumberOfArguments
		}
		timeout, err := strconv.ParseFloat(arg, 64)
		if err != nil || timeout < 0 {
			return NOMessage, errInvalidArgument(arg)
		}
		client.timeout = time.Duration(timeout * float64(time.Second))
		return OKMessage(msg, start), nil
	}
	// return the timeout
	switch msg.OutputType {
	default:
		return NOMessage, nil
	case JSON:
		return resp.StringValue(`{"ok":true` +
			`,"seconds":` + strconv.FormatFloat(client.timeout.Seconds(), 'f', -1, 64) +
			`,"elapsed":` + time.Now().Sub(start).String() + `}`), nil
	case RESP:
		return resp.FloatValue(client.timeout.Seconds()), nil
	}
}
