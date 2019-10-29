package server

import (
	"math/rand"
	"time"

	"github.com/tidwall/rhh"
	"github.com/tidwall/tile38/internal/log"
)

// clearIDExpires clears a single item from the expires list.
func (c *Server) clearIDExpires(key, id string) (cleared bool) {
	if c.expires.Len() > 0 {
		if idm, ok := c.expires.Get(key); ok {
			if _, ok := idm.(*rhh.Map).Delete(id); ok {
				if idm.(*rhh.Map).Len() == 0 {
					c.expires.Delete(key)
				}
				return true
			}
		}
	}
	return false
}

// clearKeyExpires clears all items that are marked as expires from a single key.
func (c *Server) clearKeyExpires(key string) {
	c.expires.Delete(key)
}

// moveKeyExpires moves all items that are marked as expires from a key to a newKey.
func (c *Server) moveKeyExpires(key, newKey string) {
	if idm, ok := c.expires.Delete(key); ok {
		c.expires.Set(newKey, idm)
	}
}

// expireAt marks an item as expires at a specific time.
func (c *Server) expireAt(key, id string, at time.Time) {
	idm, ok := c.expires.Get(key)
	if !ok {
		idm = rhh.New(0)
		c.expires.Set(key, idm)
	}
	idm.(*rhh.Map).Set(id, at.UnixNano())
}

// getExpires returns the when an item expires.
func (c *Server) getExpires(key, id string) (at time.Time, ok bool) {
	if c.expires.Len() > 0 {
		if idm, ok := c.expires.Get(key); ok {
			if atv, ok := idm.(*rhh.Map).Get(id); ok {
				return time.Unix(0, atv.(int64)), true
			}
		}
	}
	return time.Time{}, false
}

// hasExpired returns true if an item has expired.
func (c *Server) hasExpired(key, id string) bool {
	if at, ok := c.getExpires(key, id); ok {
		return time.Now().After(at)
	}
	return false
}

const bgExpireDelay = time.Second / 10
const bgExpireSegmentSize = 20

// expirePurgeSweep is ran from backgroundExpiring operation and performs
// segmented sweep of the expires list
func (c *Server) expirePurgeSweep(rng *rand.Rand) (purged int) {
	now := time.Now().UnixNano()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.expires.Len() == 0 {
		return 0
	}
	for i := 0; i < bgExpireSegmentSize; i++ {
		if key, idm, ok := c.expires.GetPos(rng.Uint64()); ok {
			id, atv, ok := idm.(*rhh.Map).GetPos(rng.Uint64())
			if ok {
				if now > atv.(int64) {
					// expired, purge from database
					msg := &Message{}
					msg.Args = []string{"del", key, id}
					_, d, err := c.cmdDel(msg)
					if err != nil {
						log.Fatal(err)
					}
					if err := c.writeAOF(msg.Args, &d); err != nil {
						log.Fatal(err)
					}
					purged++
				}
			}
		}
		// recycle the lock
		c.mu.Unlock()
		c.mu.Lock()
	}
	return purged
}

// backgroundExpiring watches for when items that have expired must be purged
// from the database. It's executes 10 times a seconds.
func (c *Server) backgroundExpiring() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		if c.stopServer.on() {
			return
		}
		purged := c.expirePurgeSweep(rng)
		if purged > bgExpireSegmentSize/4 {
			// do another purge immediately
			continue
		} else {
			// back off
			time.Sleep(bgExpireDelay)
		}
	}
}
