package server

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/field"
	"github.com/tidwall/tile38/internal/glob"
)

// type fvt struct {
// 	field string
// 	value float64
// }

// func orderFields(fmap map[string]int, farr []string, fields []float64) []fvt {
// 	var fv fvt
// 	var idx int
// 	fvs := make([]fvt, 0, len(fmap))
// 	for _, field := range farr {
// 		idx = fmap[field]
// 		if idx < len(fields) {
// 			fv.field = field
// 			fv.value = fields[idx]
// 			if fv.value != 0 {
// 				fvs = append(fvs, fv)
// 			}
// 		}
// 	}
// 	return fvs
// }

func (s *Server) cmdBounds(msg *Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var key string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return NOMessage, errKeyNotFound
	}

	vals := make([]resp.Value, 0, 2)
	var buf bytes.Buffer
	if msg.OutputType == JSON {
		buf.WriteString(`{"ok":true`)
	}
	minX, minY, maxX, maxY := col.Bounds()

	bbox := geojson.NewRect(geometry.Rect{
		Min: geometry.Point{X: minX, Y: minY},
		Max: geometry.Point{X: maxX, Y: maxY},
	})
	if msg.OutputType == JSON {
		buf.WriteString(`,"bounds":`)
		buf.WriteString(string(bbox.AppendJSON(nil)))
	} else {
		vals = append(vals, resp.ArrayValue([]resp.Value{
			resp.ArrayValue([]resp.Value{
				resp.FloatValue(minX),
				resp.FloatValue(minY),
			}),
			resp.ArrayValue([]resp.Value{
				resp.FloatValue(maxX),
				resp.FloatValue(maxY),
			}),
		}))
	}
	switch msg.OutputType {
	case JSON:
		buf.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		return vals[0], nil
	}
	return NOMessage, nil
}

func (s *Server) cmdType(msg *Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var key string
	if _, key, ok = tokenval(vs); !ok || key == "" {
		return NOMessage, errInvalidNumberOfArguments
	}

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.SimpleStringValue("none"), nil
		}
		return NOMessage, errKeyNotFound
	}

	typ := "hash"

	switch msg.OutputType {
	case JSON:
		return resp.StringValue(`{"ok":true,"type":` + string(typ) + `,"elapsed":"` + time.Since(start).String() + "\"}"), nil
	case RESP:
		return resp.SimpleStringValue(typ), nil
	}
	return NOMessage, nil
}

func (s *Server) cmdGet(msg *Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var key, id, typ, sprecision string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if vs, id, ok = tokenval(vs); !ok || id == "" {
		return NOMessage, errInvalidNumberOfArguments
	}

	withfields := false
	if _, peek, ok := tokenval(vs); ok && strings.ToLower(peek) == "withfields" {
		withfields = true
		vs = vs[1:]
	}

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return NOMessage, errKeyNotFound
	}
	o, fields, _, ok := col.Get(id)
	if !ok {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return NOMessage, errIDNotFound
	}

	vals := make([]resp.Value, 0, 2)
	var buf bytes.Buffer
	if msg.OutputType == JSON {
		buf.WriteString(`{"ok":true`)
	}
	vs, typ, ok = tokenval(vs)
	typ = strings.ToLower(typ)
	if !ok {
		typ = "object"
	}
	switch typ {
	default:
		return NOMessage, errInvalidArgument(typ)
	case "object":
		if msg.OutputType == JSON {
			buf.WriteString(`,"object":`)
			buf.WriteString(string(o.AppendJSON(nil)))
		} else {
			vals = append(vals, resp.StringValue(o.String()))
		}
	case "point":
		if msg.OutputType == JSON {
			buf.WriteString(`,"point":`)
			buf.Write(appendJSONSimplePoint(nil, o))
		} else {
			point := o.Center()
			z := extractZCoordinate(o)
			if z != 0 {
				vals = append(vals, resp.ArrayValue([]resp.Value{
					resp.StringValue(strconv.FormatFloat(point.Y, 'f', -1, 64)),
					resp.StringValue(strconv.FormatFloat(point.X, 'f', -1, 64)),
					resp.StringValue(strconv.FormatFloat(z, 'f', -1, 64)),
				}))
			} else {
				vals = append(vals, resp.ArrayValue([]resp.Value{
					resp.StringValue(strconv.FormatFloat(point.Y, 'f', -1, 64)),
					resp.StringValue(strconv.FormatFloat(point.X, 'f', -1, 64)),
				}))
			}
		}
	case "hash":
		if vs, sprecision, ok = tokenval(vs); !ok || sprecision == "" {
			return NOMessage, errInvalidNumberOfArguments
		}
		if msg.OutputType == JSON {
			buf.WriteString(`,"hash":`)
		}
		precision, err := strconv.ParseInt(sprecision, 10, 64)
		if err != nil || precision < 1 || precision > 12 {
			return NOMessage, errInvalidArgument(sprecision)
		}
		center := o.Center()
		p := geohash.EncodeWithPrecision(center.Y, center.X, uint(precision))
		if msg.OutputType == JSON {
			buf.WriteString(`"` + p + `"`)
		} else {
			vals = append(vals, resp.StringValue(p))
		}
	case "bounds":
		if msg.OutputType == JSON {
			buf.WriteString(`,"bounds":`)
			buf.Write(appendJSONSimpleBounds(nil, o))
		} else {
			bbox := o.Rect()
			vals = append(vals, resp.ArrayValue([]resp.Value{
				resp.ArrayValue([]resp.Value{
					resp.FloatValue(bbox.Min.Y),
					resp.FloatValue(bbox.Min.X),
				}),
				resp.ArrayValue([]resp.Value{
					resp.FloatValue(bbox.Max.Y),
					resp.FloatValue(bbox.Max.X),
				}),
			}))
		}
	}

	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	if withfields {
		nfields := fields.Len()
		if nfields > 0 {
			fvals := make([]resp.Value, 0, nfields*2)
			if msg.OutputType == JSON {
				buf.WriteString(`,"fields":{`)
			}
			var i int
			fields.Scan(func(f field.Field) bool {
				if msg.OutputType == JSON {
					if i > 0 {
						buf.WriteString(`,`)
					}
					buf.WriteString(jsonString(f.Name()) + ":" + f.Value().JSON())
				} else {
					fvals = append(fvals,
						resp.StringValue(f.Name()), resp.StringValue(f.Value().Data()))
				}
				i++
				return true
			})
			if msg.OutputType == JSON {
				buf.WriteString(`}`)
			} else {
				vals = append(vals, resp.ArrayValue(fvals))
			}
		}
	}
	switch msg.OutputType {
	case JSON:
		buf.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		var oval resp.Value
		if withfields {
			oval = resp.ArrayValue(vals)
		} else {
			oval = vals[0]
		}
		return oval, nil
	}
	return NOMessage, nil
}

func (s *Server) cmdDel(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	erron404 := false
	if len(vs) > 0 {
		_, arg, ok := tokenval(vs)
		if ok && strings.ToLower(arg) == "erron404" {
			erron404 = true
			vs = vs[1:]
		} else {
			err = errInvalidArgument(arg)
			return
		}
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	found := false
	col, _ := s.cols.Get(d.key)
	if col != nil {
		d.obj, d.fields, ok = col.Delete(d.id)
		if ok {
			if col.Count() == 0 {
				s.cols.Delete(d.key)
			}
			found = true
		} else if erron404 {
			err = errIDNotFound
			return
		}
	} else if erron404 {
		err = errKeyNotFound
		return
	}
	s.groupDisconnectObject(d.key, d.id)
	d.command = "del"
	d.updated = found
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (s *Server) cmdPdel(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.pattern, ok = tokenval(vs); !ok || d.pattern == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	now := time.Now()
	iter := func(id string, o geojson.Object, fields field.List) bool {
		if match, _ := glob.Match(d.pattern, id); match {
			d.children = append(d.children, &commandDetails{
				command:   "del",
				updated:   true,
				timestamp: now,
				key:       d.key,
				id:        id,
			})
		}
		return true
	}

	var expired int
	col, _ := s.cols.Get(d.key)
	if col != nil {
		g := glob.Parse(d.pattern, false)
		if g.Limits[0] == "" && g.Limits[1] == "" {
			col.Scan(false, nil, msg.Deadline, iter)
		} else {
			col.ScanRange(g.Limits[0], g.Limits[1], false, nil, msg.Deadline, iter)
		}
		var atLeastOneNotDeleted bool
		for i, dc := range d.children {
			dc.obj, dc.fields, ok = col.Delete(dc.id)
			if !ok {
				d.children[i].command = "?"
				atLeastOneNotDeleted = true
			} else {
				d.children[i] = dc
			}
			s.groupDisconnectObject(dc.key, dc.id)
		}
		if atLeastOneNotDeleted {
			var nchildren []*commandDetails
			for _, dc := range d.children {
				if dc.command == "del" {
					nchildren = append(nchildren, dc)
				}
			}
			d.children = nchildren
		}
		if col.Count() == 0 {
			s.cols.Delete(d.key)
		}
	}
	d.command = "pdel"
	d.updated = len(d.children) > 0
	d.timestamp = now
	d.parent = true
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		total := len(d.children) - expired
		if total < 0 {
			total = 0
		}
		res = resp.IntegerValue(total)
	}
	return
}

func (s *Server) cmdDrop(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	col, _ := s.cols.Get(d.key)
	if col != nil {
		s.cols.Delete(d.key)
		d.updated = true
	} else {
		d.key = "" // ignore the details
		d.updated = false
	}
	s.groupDisconnectCollection(d.key)
	d.command = "drop"
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (s *Server) cmdRename(msg *Message) (res resp.Value, d commandDetails, err error) {
	nx := msg.Command() == "renamenx"
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.newKey, ok = tokenval(vs); !ok || d.newKey == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	col, _ := s.cols.Get(d.key)
	if col == nil {
		err = errKeyNotFound
		return
	}
	s.hooks.Ascend(nil, func(v interface{}) bool {
		h := v.(*Hook)
		if h.Key == d.key || h.Key == d.newKey {
			err = errKeyHasHooksSet
			return false
		}
		return true
	})
	d.command = "rename"
	newCol, _ := s.cols.Get(d.newKey)
	if newCol == nil {
		d.updated = true
	} else if nx {
		d.updated = false
	} else {
		s.cols.Delete(d.newKey)
		d.updated = true
	}
	if d.updated {
		s.cols.Delete(d.key)
		s.cols.Set(d.newKey, col)
	}
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		if !nx {
			res = resp.SimpleStringValue("OK")
		} else if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (s *Server) cmdFLUSHDB(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}

	// clear the entire database
	s.cols.Clear()
	s.groupHooks.Clear()
	s.groupObjects.Clear()
	s.hookExpires.Clear()
	s.hooks.Clear()
	s.hooksOut.Clear()
	s.hookTree.Clear()
	s.hookCross.Clear()

	d.command = "flushdb"
	d.updated = true
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		res = resp.SimpleStringValue("OK")
	}
	return
}

// SET key id [FIELD name value ...] [EX seconds] [NX|XX]
// (OBJECT geojson)|(POINT lat lon z)|(BOUNDS minlat minlon maxlat maxlon)|(HASH geohash)|(STRING value)
func (s *Server) cmdSET(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()
	if s.config.maxMemory() > 0 && s.outOfMemory.on() {
		return retwerr(errOOM)
	}

	// >> Args

	var key string
	var id string
	var fields []field.Field
	var ex int64
	var xx bool
	var nx bool
	var obj geojson.Object

	args := msg.Args
	if len(args) < 3 {
		return retwerr(errInvalidNumberOfArguments)
	}

	key, id = args[1], args[2]

	for i := 3; i < len(args); i++ {
		switch strings.ToLower(args[i]) {
		case "field":
			if i+2 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			fkey := strings.ToLower(args[i+1])
			fval := args[i+2]
			i += 2
			if isReservedFieldName(fkey) {
				return retwerr(errInvalidArgument(fkey))
			}
			fields = append(fields, field.Make(fkey, fval))
		case "ex":
			if i+1 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			exval := args[i+1]
			i += 1
			x, err := strconv.ParseFloat(exval, 64)
			if err != nil {
				return retwerr(errInvalidArgument(exval))
			}
			ex = time.Now().UnixNano() + int64(float64(time.Second)*x)
		case "nx":
			if xx {
				return retwerr(errInvalidArgument(args[i]))
			}
			nx = true
		case "xx":
			if nx {
				return retwerr(errInvalidArgument(args[i]))
			}
			xx = true
		case "string":
			if i+1 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			str := args[i+1]
			i += 1
			obj = collection.String(str)
		case "point":
			if i+2 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			slat := args[i+1]
			slon := args[i+2]
			i += 2
			var z float64
			var hasZ bool
			if i+1 < len(args) {
				// probe for possible z coordinate
				var err error
				z, err = strconv.ParseFloat(args[i+1], 64)
				if err == nil {
					hasZ = true
					i++
				}
			}
			y, err := strconv.ParseFloat(slat, 64)
			if err != nil {
				return retwerr(errInvalidArgument(slat))
			}
			x, err := strconv.ParseFloat(slon, 64)
			if err != nil {
				return retwerr(errInvalidArgument(slon))
			}
			if !hasZ {
				obj = geojson.NewPoint(geometry.Point{X: x, Y: y})
			} else {
				obj = geojson.NewPointZ(geometry.Point{X: x, Y: y}, z)
			}
		case "bounds":
			if i+4 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			var vals [4]float64
			for j := 0; j < 4; j++ {
				var err error
				vals[j], err = strconv.ParseFloat(args[i+1+j], 64)
				if err != nil {
					return retwerr(errInvalidArgument(args[i+1+j]))
				}
			}
			i += 4
			obj = geojson.NewRect(geometry.Rect{
				Min: geometry.Point{X: vals[1], Y: vals[0]},
				Max: geometry.Point{X: vals[3], Y: vals[2]},
			})
		case "hash":
			if i+1 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			shash := args[i+1]
			i += 1
			lat, lon := geohash.Decode(shash)
			obj = geojson.NewPoint(geometry.Point{X: lon, Y: lat})
		case "object":
			if i+1 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			json := args[i+1]
			i += 1
			var err error
			obj, err = geojson.Parse(json, &s.geomParseOpts)
			if err != nil {
				return retwerr(err)
			}
		default:
			return retwerr(errInvalidArgument(args[i]))
		}
	}

	// >> Operation

	var nada bool
	col, ok := s.cols.Get(key)
	if !ok {
		if xx {
			nada = true
		} else {
			col = collection.New()
			s.cols.Set(key, col)
		}
	}

	var ofields field.List
	if !nada {
		_, ofields, _, ok = col.Get(id)
		if xx || nx {
			if (nx && ok) || (xx && !ok) {
				nada = true
			}
		}
	}

	if nada {
		// exclude operation due to 'xx' or 'nx' match
		switch msg.OutputType {
		default:
		case JSON:
			if nx {
				return retwerr(errIDAlreadyExists)
			} else {
				return retwerr(errIDNotFound)
			}
		case RESP:
			return resp.NullValue(), commandDetails{}, nil
		}
		return retwerr(errors.New("nada unknown output"))
	}

	for _, f := range fields {
		ofields = ofields.Set(f)
	}

	oldObj, oldFields, newFields := col.Set(id, obj, ofields, ex)

	// >> Response

	var d commandDetails
	d.command = "set"
	d.key = key
	d.id = id
	d.obj = obj
	d.oldObj = oldObj
	d.oldFields = oldFields
	d.fields = newFields
	d.updated = true // perhaps we should do a diff on the previous object?
	d.timestamp = time.Now()

	var res resp.Value
	switch msg.OutputType {
	default:
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		res = resp.SimpleStringValue("OK")
	}
	return res, d, nil
}

func retwerr(err error) (resp.Value, commandDetails, error) {
	return resp.Value{}, commandDetails{}, err
}
func retrerr(err error) (resp.Value, error) {
	return resp.Value{}, err
}

// FSET key id [XX] field value [field value...]
func (s *Server) cmdFSET(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()
	if s.config.maxMemory() > 0 && s.outOfMemory.on() {
		return retwerr(errOOM)
	}

	// >> Args

	var id string
	var key string
	var xx bool
	var fields []field.Field // raw fields

	args := msg.Args
	if len(args) < 5 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key, id = args[1], args[2]
	for i := 3; i < len(args); i++ {
		arg := strings.ToLower(args[i])
		switch arg {
		case "xx":
			xx = true
		default:
			fkey := arg
			i++
			if i == len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			if isReservedFieldName(fkey) {
				return retwerr(errInvalidArgument(fkey))
			}
			fval := args[i]
			fields = append(fields, field.Make(fkey, fval))
		}
	}

	// >> Operation

	var d commandDetails
	var updateCount int

	col, ok := s.cols.Get(key)
	if !ok {
		return retwerr(errKeyNotFound)
	}
	obj, ofields, ex, ok := col.Get(id)
	if !(ok || xx) {
		return retwerr(errIDNotFound)
	}

	if ok {
		for _, f := range fields {
			prev := ofields.Get(f.Name())
			if !prev.Value().Equals(f.Value()) {
				ofields = ofields.Set(f)
				updateCount++
			}
		}
		col.Set(id, obj, ofields, ex)
		d.obj = obj
		d.command = "fset"
		d.key = key
		d.id = id
		d.timestamp = time.Now()
		d.updated = updateCount > 0
	}

	// >> Response

	var res resp.Value

	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		res = resp.IntegerValue(updateCount)
	}

	return res, d, nil
}

// EXPIRE key id seconds
func (s *Server) cmdEXPIRE(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()
	args := msg.Args
	if len(args) != 4 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key, id, svalue := args[1], args[2], args[3]
	value, err := strconv.ParseFloat(svalue, 64)
	if err != nil {
		return retwerr(errInvalidArgument(svalue))
	}
	var ok bool
	col, _ := s.cols.Get(key)
	if col != nil {
		// replace the expiration by getting the old objec
		ex := time.Now().Add(time.Duration(float64(time.Second) * value)).UnixNano()
		var obj geojson.Object
		var fields field.List
		obj, fields, _, ok = col.Get(id)
		if ok {
			col.Set(id, obj, fields, ex)
		}
	}
	var d commandDetails
	if ok {
		d.key = key
		d.id = id
		d.command = "expire"
		d.updated = true
		d.timestamp = time.Now()
	}
	var res resp.Value
	switch msg.OutputType {
	case JSON:
		if ok {
			res = resp.StringValue(`{"ok":true,"elapsed":"` +
				time.Since(start).String() + "\"}")
		} else if col == nil {
			return retwerr(errKeyNotFound)
		} else {
			return retwerr(errIDNotFound)
		}
	case RESP:
		if ok {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return res, d, nil
}

// PERSIST key id
func (s *Server) cmdPERSIST(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()
	args := msg.Args
	if len(args) != 3 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key, id := args[1], args[2]
	var cleared bool
	var ok bool
	col, _ := s.cols.Get(key)
	if col != nil {
		var ex int64
		_, _, ex, ok = col.Get(id)
		if ok && ex != 0 {
			var obj geojson.Object
			var fields field.List
			obj, fields, _, ok = col.Get(id)
			if ok {
				col.Set(id, obj, fields, 0)
			}
			if ok {
				cleared = true
			}
		}
	}

	if !ok {
		if msg.OutputType == RESP {
			return resp.IntegerValue(0), commandDetails{}, nil
		}
		if col == nil {
			return retwerr(errKeyNotFound)
		}
		return retwerr(errIDNotFound)
	}

	var res resp.Value

	var d commandDetails
	d.key = key
	d.id = id
	d.command = "persist"
	d.updated = cleared
	d.timestamp = time.Now()

	switch msg.OutputType {
	case JSON:
		res = resp.SimpleStringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		if cleared {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return res, d, nil
}

// TTL key id
func (s *Server) cmdTTL(msg *Message) (resp.Value, error) {
	start := time.Now()
	args := msg.Args
	if len(args) != 3 {
		return retrerr(errInvalidNumberOfArguments)
	}
	key, id := args[1], args[2]
	var v float64
	var ok bool
	var ok2 bool
	col, _ := s.cols.Get(key)
	if col != nil {
		var ex int64
		_, _, ex, ok = col.Get(id)
		if ok {
			if ex != 0 {
				now := start.UnixNano()
				if now > ex {
					ok2 = false
				} else {
					v = float64(ex-now) / float64(time.Second)
					if v < 0 {
						v = 0
					}
					ok2 = true
				}
			}
		}
	}
	var res resp.Value
	switch msg.OutputType {
	case JSON:
		if ok {
			var ttl string
			if ok2 {
				ttl = strconv.FormatFloat(v, 'f', -1, 64)
			} else {
				ttl = "-1"
			}
			res = resp.SimpleStringValue(
				`{"ok":true,"ttl":` + ttl + `,"elapsed":"` +
					time.Since(start).String() + "\"}")
		} else {
			if col == nil {
				return retrerr(errKeyNotFound)
			}
			return retrerr(errIDNotFound)
		}
	case RESP:
		if ok {
			if ok2 {
				res = resp.IntegerValue(int(v))
			} else {
				res = resp.IntegerValue(-1)
			}
		} else {
			res = resp.IntegerValue(-2)
		}
	}
	return res, nil
}
