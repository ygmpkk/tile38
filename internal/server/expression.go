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
)

// areaExpression is either an object or operator+children
type areaExpression struct {
	negate bool
	obj geojson.Object
	op BinaryOp
	children children
}

type children []*areaExpression

func (e *areaExpression) String() string {
	if e.obj != nil {
		return e.obj.String()
	}
	var chStrings []string
	for _, c := range e.children {
		chStrings = append(chStrings, c.String())
	}
	switch e.op {
	case NOOP:
		return "empty operator"
	case AND:
		return "(" + strings.Join(chStrings, " AND ") + ")"
	case OR:
		return "(" + strings.Join(chStrings, " OR ") + ")"
	}
	return "unknown operator"
}

// Return boolean value modulo negate field of the expression.
func (e *areaExpression) booleanize(val bool) bool {
	if e.negate {
		return !val
	}
	return val
}

func (e *areaExpression) Intersects(o geojson.Object) bool {
	if e.obj != nil {
		return e.booleanize(e.obj.Intersects(o))
	}
	switch e.op {
	case AND:
		for _, c := range e.children {
			if !c.Intersects(o) {
				return e.booleanize(false)
			}
		}
		return e.booleanize(true)
	case OR:
		for _, c := range e.children {
			if c.Intersects(o) {
				return e.booleanize(true)
			}
		}
		return e.booleanize(false)
	}
	return e.booleanize(false)
}

// object within an expression means anything of this expression contains object
func (e *areaExpression) Contains(o geojson.Object) bool {
	if e.obj != nil {
		return e.booleanize(e.obj.Contains(o))
	}
	switch e.op {
	case AND:
		for _, c:= range e.children {
			if !c.Contains(o) {
				return e.booleanize(false)
			}
		}
		return e.booleanize(true)
	case OR:
		for _, c:= range e.children {
			if c.Contains(o) {
				return e.booleanize(true)
			}
		}
		return e.booleanize(false)
	}
	return e.booleanize(false)
}

func (e *areaExpression) Within(o geojson.Object) bool {
	if e.obj != nil {
		return e.booleanize(e.obj.Within(o))
	}
	switch e.op {
	case AND:
		for _, c:= range e.children {
			if !c.Within(o) {
				return e.booleanize(false)
			}
		}
		return e.booleanize(true)
	case OR:
		for _, c:= range e.children {
			if c.Within(o) {
				return e.booleanize(true)
			}
		}
		return e.booleanize(false)
	}
	return e.booleanize(false)
}

func (e *areaExpression) IntersectsExpr(oe *areaExpression) bool {
	if oe.obj != nil {
		return oe.booleanize(e.Intersects(oe.obj))
	}
	switch oe.op {
	case AND:
		for _, c := range oe.children {
			if !e.IntersectsExpr(c) {
				return e.booleanize(false)
			}
		}
		return e.booleanize(true)
	case OR:
		for _, c := range oe.children {
			if e.IntersectsExpr(c) {
				return e.booleanize(true)
			}
		}
		return e.booleanize(false)
	}
	return e.booleanize(false)

}

func (e *areaExpression) WithinExpr(oe *areaExpression) bool {
	if oe.obj != nil {
		return oe.booleanize(e.Within(oe.obj))
	}
	switch oe.op {
	case AND:
		for _, c:= range oe.children {
			if !e.WithinExpr(c) {
				return e.booleanize(false)
			}
		}
		return e.booleanize(true)
	case OR:
		for _, c:= range oe.children {
			if e.WithinExpr(c) {
				return e.booleanize(true)
			}
		}
		return e.booleanize(false)
	}
	return e.booleanize(false)
}

func (e *areaExpression) ContainsExpr(oe *areaExpression) bool {
	if oe.obj != nil {
		return oe.booleanize(e.Contains(oe.obj))
	}
	switch oe.op {
	case AND:
		for _, c:= range oe.children {
			if !e.ContainsExpr(c) {
				return e.booleanize(false)
			}
		}
		return e.booleanize(true)
	case OR:
		for _, c:= range oe.children {
			if e.ContainsExpr(c) {
				return e.booleanize(true)
			}
		}
		return e.booleanize(false)
	}
	return e.booleanize(false)
}
