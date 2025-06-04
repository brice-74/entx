package search

import (
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
)

type BaseNode struct {
	name      string
	tableName string
	bridges   map[string]Bridge
	fields    map[string]*Field // keys are completed by ent name
	pks       []*Field
}

type Field struct {
	Name        string
	StorageName string
}

func NewBaseNode(
	name string,
	tableName string,
	bridges map[string]Bridge,
	fields map[string]*Field,
	pks []*Field,
) BaseNode {
	return BaseNode{
		name,
		tableName,
		bridges,
		fields,
		pks,
	}
}

func (n *BaseNode) PKs() []*Field {
	return n.pks
}

func (n *BaseNode) Name() string {
	return n.name
}

func (n *BaseNode) Table() string {
	return n.tableName
}

func (n *BaseNode) Bridge(s string) Bridge {
	return n.bridges[s]
}

func (n *BaseNode) SetBridge(key string, bridge Bridge) {
	n.bridges[key] = bridge
}

func (n *BaseNode) FieldByName(s string) *Field {
	return n.fields[s]
}

type BaseBridge struct {
	parent  Node
	child   Node
	inverse Bridge
	RelationInfos
}

func NewBaseBridge(
	parent Node,
	child Node,
	inverse Bridge,
	relInfos RelationInfos,
) BaseBridge {
	return BaseBridge{
		parent,
		child,
		inverse,
		relInfos,
	}
}

func (b *BaseBridge) JoinPivot(s *sql.Selector, outer ...*sql.SelectTable) *sql.SelectTable {
	var c func(string) string
	if len(outer) > 0 && outer[0] != nil {
		c = outer[0].C
	} else {
		c = s.C
	}

	if b.RelType == sqlgraph.M2M {
		pivot := sql.Table(b.PivotTable)

		s.Join(pivot).
			On(pivot.C(b.PivotLeftField), c(b.FinalRightField))

		return pivot
	}
	return nil
}

func (b *BaseBridge) Join(s *sql.Selector, outer ...*sql.SelectTable) (tables []*sql.SelectTable) {
	var c func(string) string
	if len(outer) > 0 && outer[0] != nil {
		c = outer[0].C
	} else {
		c = s.C
	}

	switch b.RelType {
	case sqlgraph.M2M:
		pivot := sql.Table(b.PivotTable)
		t := sql.Table(b.child.Table())

		s.Join(pivot).
			On(pivot.C(b.PivotLeftField), c(b.FinalLeftField)).
			Join(t).
			On(pivot.C(b.PivotRightField), t.C(b.FinalRightField))

		tables = append(tables, t, pivot)
	default:
		t := sql.Table(b.child.Table())

		s.Join(t).
			On(t.C(b.FinalRightField), c(b.FinalLeftField))

		tables = append(tables, t)
	}
	return
}

func (b *BaseBridge) SetInverse(bridge Bridge) {
	b.inverse = bridge
}

func (b *BaseBridge) Inverse() Bridge {
	return b.inverse
}

func (b *BaseBridge) RelInfos() *RelationInfos {
	return &b.RelationInfos
}

func (b *BaseBridge) Child() Node {
	return b.child
}

func (b *BaseBridge) Parent() Node {
	return b.parent
}
