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
	negate   bool
	obj      geojson.Object
	op       BinaryOp
	children children
}

type children []*areaExpression

// String representation, helpful in logging.
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
func (e *areaExpression) testObject(
	o geojson.Object,
	objObjTest func(o1, o2 geojson.Object) bool,
	exprObjTest func(ae *areaExpression, ob geojson.Object) bool,
) bool {
	if e.obj != nil {
		return objObjTest(e.obj, o)
	}
	switch e.op {
	case AND:
		for _, c := range e.children {
			if !exprObjTest(c, o) {
				return false
			}
		}
		return true
	case OR:
		for _, c := range e.children {
			if exprObjTest(c, o) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *areaExpression) rawIntersects(o geojson.Object) bool {
	return e.testObject(o, geojson.Object.Intersects, (*areaExpression).Intersects)
}

func (e *areaExpression) rawContains(o geojson.Object) bool {
	return e.testObject(o, geojson.Object.Contains, (*areaExpression).Contains)
}

func (e *areaExpression) rawWithin(o geojson.Object) bool {
	return e.testObject(o, geojson.Object.Within, (*areaExpression).Within)
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
func (e *areaExpression) testExpression(
	oe *areaExpression,
	exprObjTest func(ae *areaExpression, ob geojson.Object) bool,
	rawExprExprTest func(ae1, ae2 *areaExpression) bool,
	exprExprTest func(ae1, ae2 *areaExpression) bool,
) bool {
	if oe.negate {
		e2 := &areaExpression{negate: !e.negate, obj: e.obj, op: e.op, children: e.children}
		oe2 := &areaExpression{negate: false, obj: oe.obj, op: oe.op, children: oe.children}
		return exprExprTest(e2, oe2)
	}
	if oe.obj != nil {
		return exprObjTest(e, oe.obj)
	}
	switch oe.op {
	case AND:
		for _, c := range oe.children {
			if !rawExprExprTest(e, c) {
				return false
			}
		}
		return true
	case OR:
		for _, c := range oe.children {
			if rawExprExprTest(e, c) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *areaExpression) rawIntersectsExpr(oe *areaExpression) bool {
	return e.testExpression(
		oe,
		(*areaExpression).rawIntersects,
		(*areaExpression).rawIntersectsExpr,
		(*areaExpression).IntersectsExpr)
}

func (e *areaExpression) rawWithinExpr(oe *areaExpression) bool {
	return e.testExpression(
		oe,
		(*areaExpression).rawWithin,
		(*areaExpression).rawWithinExpr,
		(*areaExpression).WithinExpr)
}

func (e *areaExpression) rawContainsExpr(oe *areaExpression) bool {
	return e.testExpression(
		oe,
		(*areaExpression).rawContains,
		(*areaExpression).rawContainsExpr,
		(*areaExpression).ContainsExpr)
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
