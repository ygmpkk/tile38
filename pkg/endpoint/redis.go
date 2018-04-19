package endpoint

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	redisExpiresAfter = time.Second * 30
)

// RedisConn is an endpoint connection
type RedisConn struct {
	mu   sync.Mutex
	ep   Endpoint
	ex   bool
	t    time.Time
	conn net.Conn
	rd   *bufio.Reader
}

func newRedisConn(ep Endpoint) *RedisConn {
	return &RedisConn{
		ep: ep,
		t:  time.Now(),
	}
}

// Expired returns true if the connection has expired
func (conn *RedisConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Now().Sub(conn.t) > redisExpiresAfter {
			if conn.conn != nil {
				conn.close()
			}
			conn.ex = true
		}
	}
	return conn.ex
}

func (conn *RedisConn) close() {
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
	}
	conn.rd = nil
}

// Send sends a message
func (conn *RedisConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.ex {
		return errExpired
	}

	conn.t = time.Now()
	if conn.conn == nil {
		addr := fmt.Sprintf("%s:%d", conn.ep.Redis.Host, conn.ep.Redis.Port)
		var err error
		conn.conn, err = net.Dial("tcp", addr)
		if err != nil {
			return err
		}
		conn.rd = bufio.NewReader(conn.conn)
	}

	var args []string
	args = append(args, "PUBLISH", conn.ep.Redis.Channel, msg)
	cmd := buildRedisCommand(args)

	if _, err := conn.conn.Write(cmd); err != nil {
		conn.close()
		return err
	}

	c, err := conn.rd.ReadByte()
	if err != nil {
		conn.close()
		return err
	}

	if c != ':' {
		conn.close()
		return errors.New("invalid redis reply")
	}

	ln, err := conn.rd.ReadBytes('\n')
	if err != nil {
		conn.close()
		return err
	}

	if string(ln[0:1]) != "1" {
		conn.close()
		return errors.New("invalid redis reply")
	}

	return nil
}
