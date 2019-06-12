package server

import (
	"strings"

	"github.com/tidwall/geojson"
)

type BinaryOp byte

const (
	NOOP BinaryOp = iota
	AND
	OR
	tokenAND    = "and"
	tokenOR     = "or"
	tokenNOT    = "not"
	tokenLParen = "("
	tokenRParen = ")"
)

// areaExpression is either an object or operator+children
type areaExpression struct {
	negate bool
	obj geojson.Object
	op BinaryOp
	children children
}

type children []*areaExpression

func (e *areaExpression) String() (res string) {
	if e.obj != nil {
		res = e.obj.String()
	} else {
		var chStrings []string
		for _, c := range e.children {
			chStrings = append(chStrings, c.String())
		}
		switch e.op {
		case NOOP:
			res = "empty operator"
		case AND:
			res = "(" + strings.Join(chStrings, " "+tokenAND+" ") + ")"
		case OR:
			res = "(" + strings.Join(chStrings, " "+tokenOR+" ") + ")"
		default:
			res = "unknown operator"
		}
	}
	if e.negate {
		res = tokenNOT + " " + res
	}
	return
}

// Return boolean value modulo negate field of the expression.
func (e *areaExpression) maybeNegate(val bool) bool {
	if e.negate {
		return !val
	}
	return val
}

// Methods for testing an areaExpression against the spatial object
func (e *areaExpression) rawIntersects(o geojson.Object) bool {
	if e.obj != nil {
		return e.obj.Intersects(o)
	}
	switch e.op {
	case AND:
		for _, c := range e.children {
			if !c.Intersects(o) {
				return false
			}
		}
		return true
	case OR:
		for _, c := range e.children {
			if c.Intersects(o) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *areaExpression) rawContains(o geojson.Object) bool {
	if e.obj != nil {
		return e.obj.Contains(o)
	}
	switch e.op {
	case AND:
		for _, c:= range e.children {
			if !c.Contains(o) {
				return false
			}
		}
		return true
	case OR:
		for _, c:= range e.children {
			if c.Contains(o) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *areaExpression) rawWithin(o geojson.Object) bool {
	if e.obj != nil {
		return e.obj.Within(o)
	}
	switch e.op {
	case AND:
		for _, c:= range e.children {
			if !c.Within(o) {
				return false
			}
		}
		return true
	case OR:
		for _, c:= range e.children {
			if c.Within(o) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *areaExpression) Intersects(o geojson.Object) bool {
	return e.maybeNegate(e.rawIntersects(o))
}

func (e *areaExpression) Contains(o geojson.Object) bool {
	return e.maybeNegate(e.rawContains(o))
}

func (e *areaExpression) Within(o geojson.Object) bool {
	return e.maybeNegate(e.rawWithin(o))
}

// Methods for testing an areaExpression against another areaExpression
func (e *areaExpression) rawIntersectsExpr(oe *areaExpression) bool {
	if oe.negate {
		e2 := &areaExpression{negate:!e.negate, obj:e.obj, op: e.op, children:e.children}
		oe2 := &areaExpression{negate:false, obj:oe.obj, op:oe.op, children:oe.children}
		return e2.rawIntersectsExpr(oe2)
	}
	if oe.obj != nil {
		return e.Intersects(oe.obj)
	}
	switch oe.op {
	case AND:
		for _, c := range oe.children {
			if !e.rawIntersectsExpr(c) {
				return false
			}
		}
		return true
	case OR:
		for _, c := range oe.children {
			if e.rawIntersectsExpr(c) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *areaExpression) rawWithinExpr(oe *areaExpression) bool {
	if oe.negate {
		e2 := &areaExpression{negate:!e.negate, obj:e.obj, op: e.op, children:e.children}
		oe2 := &areaExpression{negate:false, obj:oe.obj, op:oe.op, children:oe.children}
		return e2.rawWithinExpr(oe2)
	}
	if oe.obj != nil {
		return e.Within(oe.obj)
	}
	switch oe.op {
	case AND:
		for _, c:= range oe.children {
			if !e.rawWithinExpr(c) {
				return false
			}
		}
		return true
	case OR:
		for _, c:= range oe.children {
			if e.rawWithinExpr(c) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *areaExpression) rawContainsExpr(oe *areaExpression) bool {
	if oe.negate {
		e2 := &areaExpression{negate:!e.negate, obj:e.obj, op: e.op, children:e.children}
		oe2 := &areaExpression{negate:false, obj:oe.obj, op:oe.op, children:oe.children}
		return e2.rawContainsExpr(oe2)
	}
	if oe.obj != nil {
		return e.Contains(oe.obj)
	}
	switch oe.op {
	case AND:
		for _, c:= range oe.children {
			if !e.rawContainsExpr(c) {
				return false
			}
		}
		return true
	case OR:
		for _, c:= range oe.children {
			if e.rawContainsExpr(c) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *areaExpression) IntersectsExpr(oe *areaExpression) bool {
	return e.maybeNegate(e.rawIntersectsExpr(oe))
}

func (e *areaExpression) WithinExpr(oe *areaExpression) bool {
	return e.maybeNegate(e.rawWithinExpr(oe))
}

func (e *areaExpression) ContainsExpr(oe *areaExpression) bool {
	return e.maybeNegate(e.rawContainsExpr(oe))
}
