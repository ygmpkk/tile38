package tests

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/tidwall/gjson"
)

type IO struct {
	args []any
	json bool
	out  any
}

func Do(args ...any) *IO {
	return &IO{args: args}
}
func (cmd *IO) JSON() *IO {
	cmd.json = true
	return cmd
}
func (cmd *IO) String(s string) *IO {
	cmd.out = s
	return cmd
}
func (cmd *IO) Custom(fn func(s string) error) *IO {
	cmd.out = func(s string) error {
		if cmd.json {
			if !gjson.Valid(s) {
				return errors.New("invalid json")
			}
		}
		return fn(s)
	}
	return cmd
}

func (cmd *IO) OK() *IO {
	return cmd.Custom(func(s string) error {
		if cmd.json {
			if gjson.Get(s, "ok").Type != gjson.True {
				return errors.New("not ok")
			}
		} else if s != "OK" {
			return errors.New("not ok")
		}
		return nil
	})
}

type ioVisitor struct {
	fset  *token.FileSet
	ln    int
	pos   int
	got   bool
	data  string
	end   int
	done  bool
	index int
	nidx  int
	frag  string
	fpos  int
}

func (v *ioVisitor) Visit(n ast.Node) ast.Visitor {
	if n == nil || v.done {
		return nil
	}

	if v.got {
		if int(n.Pos()) > v.end {
			v.done = true
			return v
		}
		if n, ok := n.(*ast.CallExpr); ok {
			frag := strings.TrimSpace(v.data[int(n.Pos())-1 : int(n.End())])
			if _, ok := n.Fun.(*ast.Ident); ok {
				if v.index == v.nidx {
					frag = strings.TrimSpace(strings.TrimSuffix(frag, "."))
					idx := strings.IndexByte(frag, '(')
					if idx != -1 {
						frag = frag[idx:]
					}
					v.frag = frag
					v.done = true
					v.fpos = int(n.Pos())
					return v
				}
				v.nidx++
			}
		}
		return v
	}
	if int(n.Pos()) == v.pos {
		if n, ok := n.(*ast.CallExpr); ok {
			v.end = int(n.Rparen)
			v.got = true
			return v
		}
	}
	return v
}

func (cmd *IO) deepError(index int, err error) error {
	oerr := err
	werr := func(err error) error {
		return fmt.Errorf("batch[%d]: %v: %v", index, oerr, err)
	}

	// analyse stack
	_, file, ln, ok := runtime.Caller(3)
	if !ok {
		return werr(errors.New("runtime.Caller failed"))
	}

	// get the character position from line
	bdata, err := os.ReadFile(file)
	if err != nil {
		return werr(err)
	}
	data := string(bdata)

	var pos int
	var iln int
	var pln int
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			j := pln
			line := data[pln:i]
			pln = i + 1
			iln++
			if iln == ln {
				line = strings.TrimSpace(line)
				if !strings.HasPrefix(line, "return mc.DoBatch(") {
					return oerr
				}
				for ; j < len(data); j++ {
					if data[j] == 'm' {
						break
					}
				}
				pos = j + 1
				break
			}
		}
	}
	if pos == 0 {
		return oerr
	}

	fset := token.NewFileSet()
	pfile, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		return werr(err)
	}
	v := &ioVisitor{
		fset:  fset,
		ln:    ln,
		pos:   pos,
		data:  string(data),
		index: index,
	}
	ast.Walk(v, pfile)

	if v.fpos == 0 {
		return oerr
	}

	pln = 1
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			if i > v.fpos {
				break
			}
			pln++
		}
	}

	fsig := fmt.Sprintf("%s:%d", filepath.Base(file), pln)
	emsg := oerr.Error()
	if strings.HasPrefix(emsg, "expected ") &&
		strings.Contains(emsg, ", got ") {
		emsg = "" +
			"  EXPECTED: " + strings.Split(emsg, ", got ")[0][9:] + "\n" +
			"       GOT: " +
			strings.Split(emsg, ", got ")[1]
	} else {
		emsg = "" +
			"     ERROR: " + emsg
	}

	return fmt.Errorf("\n%s: entry[%d]\n   COMMAND: %s\n%s",
		fsig, index+1, v.frag, emsg)
}

func (mc *mockServer) doIOTest(index int, cmd *IO) error {
	// switch json mode if desired
	if cmd.json {
		if !mc.ioJSON {
			if _, err := mc.Do("OUTPUT", "json"); err != nil {
				return err
			}
			mc.ioJSON = true
		}
	} else {
		if mc.ioJSON {
			if _, err := mc.Do("OUTPUT", "resp"); err != nil {
				return err
			}
			mc.ioJSON = false
		}
	}

	err := mc.DoExpect(cmd.out, cmd.args[0].(string), cmd.args[1:]...)
	if err != nil {
		return cmd.deepError(index, err)
	}
	return nil
}
