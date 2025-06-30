package dsl

import (
	"fmt"
	"strings"

	"github.com/brice-74/entx"
)

var (
	ErrChainBroken = "invalid chain: field %q cannot appear before the end (in node %q)"
	ErrUnknownLink = "invalid chain: segment %q is neither a field nor a relation of node %q"
)

// resolveChain traverses a list of segments starting from a start node.
// Returns the final node, the name of the terminal field (or empty if no field) and
// the slice of bridges traversed.
func resolveChain(start entx.Node, parts []string) (current entx.Node, field string, bridges []entx.Bridge, err error) {
	current = start
	bridges = make([]entx.Bridge, 0, len(parts))
	for i, seg := range parts {
		if b := current.Bridge(seg); b != nil {
			bridges = append(bridges, b)
			current = b.Child()
		} else if f := current.FieldByName(seg); f != nil {
			if i != len(parts)-1 {
				err = fmt.Errorf(ErrChainBroken, seg, current.Name())
				return
			}
			field = seg
		} else {
			err = fmt.Errorf(ErrUnknownLink, seg, current.Name())
			return
		}
	}
	return
}

func splitChain(s string) (parts []string, invalidAt int, ok bool) {
	parts = strings.Split(s, ".")
	pos := 0
	for _, part := range parts {
		if part == "" {
			return nil, pos, false
		}
		pos += len(part) + 1
	}
	return parts, -1, true
}
