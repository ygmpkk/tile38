package controller

import (
	"math"
	"strconv"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/tile38/pkg/geojson"
	"github.com/tidwall/tile38/pkg/glob"
	"github.com/tidwall/tile38/pkg/server"
)

// FenceMatch executes a fence match returns back json messages for fence detection.
func FenceMatch(hookName string, sw *scanWriter, fence *liveFenceSwitches, metas []FenceMeta, details *commandDetailsT) []string {
	msgs := fenceMatch(hookName, sw, fence, metas, details)
	if len(fence.accept) == 0 {
		return msgs
	}
	nmsgs := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		if fence.accept[gjson.Get(msg, "command").String()] {
			nmsgs = append(nmsgs, msg)
		}
	}
	return nmsgs
}
func appendHookDetails(b []byte, hookName string, metas []FenceMeta) []byte {
	if len(hookName) > 0 {
		b = append(b, `,"hook":`...)
		b = appendJSONString(b, hookName)
	}
	if len(metas) > 0 {
		b = append(b, `,"meta":{`...)
		for i, meta := range metas {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendJSONString(b, meta.Name)
			b = append(b, ':')
			b = appendJSONString(b, meta.Value)
		}
		b = append(b, '}')
	}
	return b
}
func hookJSONString(hookName string, metas []FenceMeta) string {
	return string(appendHookDetails(nil, hookName, metas))
}
func fenceMatch(
	hookName string, sw *scanWriter, fence *liveFenceSwitches,
	metas []FenceMeta, details *commandDetailsT,
) []string {
	if details.command == "drop" {
		return []string{
			`{"command":"drop"` + hookJSONString(hookName, metas) +
				`,"time":` + jsonTimeFormat(details.timestamp) + `}`,
		}
	}
	if len(fence.glob) > 0 && !(len(fence.glob) == 1 && fence.glob[0] == '*') {
		match, _ := glob.Match(fence.glob, details.id)
		if !match {
			return nil
		}
	}
	if details.obj == nil || !details.obj.IsGeometry() {
		return nil
	}
	if details.command == "fset" {
		sw.mu.Lock()
		nofields := sw.nofields
		sw.mu.Unlock()
		if nofields {
			return nil
		}
	}
	if details.command == "del" {
		if fence.roam.on {
			if fence.roam.nearbys != nil {
				delete(fence.roam.nearbys, details.id)
				if len(fence.roam.nearbys) == 0 {
					fence.roam.nearbys = nil
				}
			}
		}
		return []string{
			`{"command":"del"` + hookJSONString(hookName, metas) +
				`,"id":` + jsonString(details.id) +
				`,"time":` + jsonTimeFormat(details.timestamp) + `}`,
		}
	}
	var roamNearbys, roamFaraways []roamMatch
	var detect = "outside"
	if fence != nil {
		if fence.roam.on {
			if details.command == "set" {
				roamNearbys, roamFaraways =
					fenceMatchRoam(sw.c, fence, details.key,
						details.id, details.obj)
			}
			if len(roamNearbys) == 0 && len(roamFaraways) == 0 {
				return nil
			}
			detect = "roam"
		} else {
			// not using roaming
			match1 := fenceMatchObject(fence, details.oldObj)
			match2 := fenceMatchObject(fence, details.obj)
			if match1 && match2 {
				detect = "inside"
			} else if match1 && !match2 {
				detect = "exit"
			} else if !match1 && match2 {
				detect = "enter"
				if details.command == "fset" {
					detect = "inside"
				}
			} else {
				if details.command != "fset" {
					// Maybe the old object and new object create a line that crosses the fence.
					// Must detect for that possibility.
					if details.oldObj != nil {
						ls := geojson.LineString{
							Coordinates: []geojson.Position{
								details.oldObj.CalculatedPoint(),
								details.obj.CalculatedPoint(),
							},
						}
						temp := false
						if fence.cmd == "within" {
							// because we are testing if the line croses the area we need to use
							// "intersects" instead of "within".
							fence.cmd = "intersects"
							temp = true
						}
						if fenceMatchObject(fence, ls) {
							detect = "cross"
						}
						if temp {
							fence.cmd = "within"
						}
					}
				}
			}
		}
	}

	if details.fmap == nil {
		return nil
	}
	for {
		if fence.detect != nil && !fence.detect[detect] {
			if detect == "enter" {
				detect = "inside"
				continue
			}
			if detect == "exit" {
				detect = "outside"
				continue
			}
			return nil
		}
		break
	}
	sw.mu.Lock()
	var distance float64
	if fence.distance {
		distance = details.obj.CalculatedPoint().DistanceTo(geojson.Position{X: fence.lon, Y: fence.lat, Z: 0})
	}
	sw.fmap = details.fmap
	sw.fullFields = true
	sw.msg.OutputType = server.JSON
	sw.writeObject(ScanWriterParams{
		id:       details.id,
		o:        details.obj,
		fields:   details.fields,
		noLock:   true,
		distance: distance,
	})

	if sw.wr.Len() == 0 {
		sw.mu.Unlock()
		return nil
	}

	res := sw.wr.String()
	sw.wr.Reset()
	if len(res) > 0 && res[0] == ',' {
		res = res[1:]
	}
	if sw.output == outputIDs {
		res = `{"id":` + string(res) + `}`
	}
	sw.mu.Unlock()

	if fence.groups == nil {
		fence.groups = make(map[string]string)
	}
	groupkey := details.key + ":" + details.id
	var group string
	var ok bool
	if detect == "enter" {
		group = bsonID()
		fence.groups[groupkey] = group
	} else if detect == "cross" {
		group = bsonID()
		delete(fence.groups, groupkey)
	} else {
		group, ok = fence.groups[groupkey]
		if !ok {
			group = bsonID()
			fence.groups[groupkey] = group
		}
	}
	var msgs []string
	if fence.detect == nil || fence.detect[detect] {
		if len(res) > 0 && res[0] == '{' {
			msgs = append(msgs, makemsg(details.command, group, detect,
				hookName, metas, details.key, details.timestamp, res[1:]))
		} else {
			msgs = append(msgs, string(res))
		}
	}
	switch detect {
	case "enter":
		if fence.detect == nil || fence.detect["inside"] {
			msgs = append(msgs, makemsg(details.command, group, "inside", hookName, metas, details.key, details.timestamp, res[1:]))
		}
	case "exit", "cross":
		if fence.detect == nil || fence.detect["outside"] {
			msgs = append(msgs, makemsg(details.command, group, "outside", hookName, metas, details.key, details.timestamp, res[1:]))
		}
	case "roam":
		if len(msgs) > 0 {
			var nmsgs []string
			for i := range roamNearbys {
				nmsg := extendRoamMessage(sw, fence,
					"nearby", msgs[0], roamNearbys[i])
				nmsgs = append(nmsgs, string(nmsg))
			}
			for i := range roamFaraways {
				nmsg := extendRoamMessage(sw, fence,
					"faraway", msgs[0], roamFaraways[i])
				nmsgs = append(nmsgs, string(nmsg))
			}
			msgs = nmsgs
		}
	}
	return msgs
}

func extendRoamMessage(
	sw *scanWriter, fence *liveFenceSwitches,
	kind string, baseMsg string, match roamMatch,
) string {
	// hack off the last '}'
	nmsg := []byte(baseMsg[:len(baseMsg)-1])
	nmsg = append(nmsg, `,"`+kind+`":{"key":`...)
	nmsg = appendJSONString(nmsg, fence.roam.key)
	nmsg = append(nmsg, `,"id":`...)
	nmsg = appendJSONString(nmsg, match.id)
	nmsg = append(nmsg, `,"object":`...)
	nmsg = append(nmsg, match.obj.JSON()...)
	nmsg = append(nmsg, `,"meters":`...)
	nmsg = strconv.AppendFloat(nmsg,
		math.Floor(match.meters*1000)/1000, 'f', -1, 64)
	if fence.roam.scan != "" {
		nmsg = append(nmsg, `,"scan":[`...)
		col := sw.c.getCol(fence.roam.key)
		if col != nil {
			obj, _, ok := col.Get(match.id)
			if ok {
				nmsg = append(nmsg,
					`{"id":`+jsonString(match.id)+
						`,"self":true,"object":`+obj.JSON()+`}`...)
			}
			pattern := match.id + fence.roam.scan
			iterator := func(
				oid string, o geojson.Object, fields []float64,
			) bool {
				if oid == match.id {
					return true
				}
				if matched, _ := glob.Match(pattern, oid); matched {
					nmsg = append(nmsg,
						`,{"id":`+jsonString(oid)+
							`,"object":`+o.JSON()+`}`...)
				}
				return true
			}
			g := glob.Parse(pattern, false)
			if g.Limits[0] == "" && g.Limits[1] == "" {
				col.Scan(false, iterator)
			} else {
				col.ScanRange(g.Limits[0], g.Limits[1],
					false, iterator)
			}
		}
		nmsg = append(nmsg, ']')
	}

	nmsg = append(nmsg, '}')

	// re-add the last '}'
	nmsg = append(nmsg, '}')
	return string(nmsg)
}

func makemsg(
	command, group, detect, hookName string,
	metas []FenceMeta, key string, t time.Time, tail string,
) string {
	var buf []byte
	buf = append(append(buf, `{"command":"`...), command...)
	buf = append(append(buf, `","group":"`...), group...)
	buf = append(append(buf, `","detect":"`...), detect...)
	buf = append(buf, '"')
	buf = appendHookDetails(buf, hookName, metas)
	buf = appendJSONString(append(buf, `,"key":`...), key)
	buf = appendJSONTimeFormat(append(buf, `,"time":`...), t)
	buf = append(append(buf, ','), tail...)
	return string(buf)
}

func fenceMatchObject(fence *liveFenceSwitches, obj geojson.Object) bool {
	if obj == nil {
		return false
	}
	if fence.roam.on {
		// we need to check this object against
		return false
	}

	if fence.cmd == "nearby" {
		return obj.Nearby(geojson.Position{X: fence.lon, Y: fence.lat, Z: 0}, fence.meters)
	}
	if fence.cmd == "within" {
		if fence.o != nil {
			return obj.Within(fence.o)
		}
		return obj.WithinBBox(geojson.BBox{
			Min: geojson.Position{X: fence.minLon, Y: fence.minLat, Z: 0},
			Max: geojson.Position{X: fence.maxLon, Y: fence.maxLat, Z: 0},
		})
	}
	if fence.cmd == "intersects" {
		if fence.o != nil {
			return obj.Intersects(fence.o)
		}
		return obj.IntersectsBBox(geojson.BBox{
			Min: geojson.Position{X: fence.minLon, Y: fence.minLat, Z: 0},
			Max: geojson.Position{X: fence.maxLon, Y: fence.maxLat, Z: 0},
		})
	}
	return false
}

func fenceMatchRoam(
	c *Controller, fence *liveFenceSwitches,
	tkey, tid string, obj geojson.Object,
) (nearbys, faraways []roamMatch) {
	col := c.getCol(fence.roam.key)
	if col == nil {
		return
	}
	p := obj.CalculatedPoint()
	prevNearbys := fence.roam.nearbys[tid]
	var newNearbys map[string]bool
	col.Nearby(0, p.Y, p.X, fence.roam.meters, math.Inf(-1), math.Inf(+1),
		func(id string, obj geojson.Object, fields []float64) bool {
			if c.hasExpired(fence.roam.key, id) {
				return true
			}
			var idMatch bool
			if id == tid {
				return true // skip self
			}
			if fence.roam.pattern {
				idMatch, _ = glob.Match(fence.roam.id, id)
			} else {
				idMatch = fence.roam.id == id
			}
			if !idMatch {
				return true
			}
			if newNearbys == nil {
				newNearbys = make(map[string]bool)
			}
			newNearbys[id] = true
			prev := prevNearbys[id]
			if prev {
				delete(prevNearbys, id)
			}

			match := roamMatch{
				id:     id,
				obj:    obj,
				meters: obj.CalculatedPoint().DistanceTo(p),
			}
			if !prev || !fence.nodwell {
				// brand new "nearby"
				nearbys = append(nearbys, match)
			}
			return true
		},
	)
	for id := range prevNearbys {
		obj, _, ok := col.Get(id)
		if ok && !c.hasExpired(fence.roam.key, id) {
			faraways = append(faraways, roamMatch{
				id: id, obj: obj,
				meters: obj.CalculatedPoint().DistanceTo(p),
			})
		}
	}

	if len(newNearbys) == 0 {
		if fence.roam.nearbys != nil {
			delete(fence.roam.nearbys, tid)
			if len(fence.roam.nearbys) == 0 {
				fence.roam.nearbys = nil
			}
		}
	} else {
		if fence.roam.nearbys == nil {
			fence.roam.nearbys = make(map[string]map[string]bool)
		}
		fence.roam.nearbys[tid] = newNearbys
	}
	return
}
