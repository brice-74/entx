package extension

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"entgo.io/ent/entc"
	"entgo.io/ent/entc/gen"
	"golang.org/x/sync/errgroup"
)

var (
	//go:embed template/*.tmpl
	_templatesFS embed.FS

	funcs = template.FuncMap{
		"searchImportPath": nil,
		"searchImportName": nil,
		"isNodeInclude":    nil,
		"debug":            debug,
		"isGenType":        isGenType,
	}
)

type Extension struct {
	entc.DefaultExtension
	conf *Config
	computedNodes
}

func New(opts ...Option) *Extension {
	ext := &Extension{conf: NewConfig(opts...)}
	// must be captured dynamically to avoid empty computedNodes
	funcs["isNodeInclude"] = func(n *gen.Type) bool { return ext.IsNodeInclude(n) }
	funcs["searchImportPath"] = func() string { return ext.conf.importPath }
	funcs["searchImportName"] = func() string { return ext.conf.importName }
	return ext
}

func (e *Extension) Templates() []*gen.Template {
	return []*gen.Template{
		e.newTemplate("additionals.tmpl"),
	}
}

func (e *Extension) Hooks() []gen.Hook {
	return []gen.Hook{
		func(next gen.Generator) gen.Generator {
			return gen.GenerateFunc(func(g *gen.Graph) error {
				e.computedNodes = e.conf.ComputeNodes(g.Nodes...)

				if err := next.Generate(g); err != nil {
					return err
				}

				entxGraph, err := e.prepareGenGraph(g)
				if err != nil {
					return err
				}

				fileInfos := []*genFileInfo{
					{template: e.newTemplate("graph.tmpl"), params: entxGraph},
					{template: e.newTemplate("adapters.tmpl"), params: g},
				}

				return genFiles(fileInfos...)
			})
		},
	}
}

func (e *Extension) newTemplate(name string) *gen.Template {
	base := filepath.Base(name)
	ext := filepath.Ext(base)
	t := gen.NewTemplate(strings.TrimSuffix(base, ext)).Funcs(funcs)
	return gen.MustParse(t.ParseFS(_templatesFS, "template/"+base))
}

type genFileInfo struct {
	template *gen.Template
	params   any
}

func genFiles(fileInfos ...*genFileInfo) error {
	if err := os.MkdirAll("entx", 0o755); err != nil {
		return fmt.Errorf("mkdir entx: %w", err)
	}

	g, ctx := errgroup.WithContext(context.Background())

	for _, fi := range fileInfos {
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				templateName := fi.template.Name()

				var buf bytes.Buffer
				if err := fi.template.ExecuteTemplate(&buf, templateName, fi.params); err != nil {
					return fmt.Errorf("execute %w", err)
				}

				outPath := "entx/" + templateName + ".go"

				if err := os.WriteFile(outPath, buf.Bytes(), 0o644); err != nil {
					return fmt.Errorf("write file %s: %w", outPath, err)
				}

				return nil
			}
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

type GenGraph struct {
	EntGraph    *gen.Graph
	Nodes       []GenNode
	BridgePairs []GenBridgePair
}

type GenNode struct {
	HasPolicy     bool
	NodeName      string
	LowerNodeName string
	TableName     string
	Columns       []*gen.Field
	PKs           []*gen.Field
	EntNode       *gen.Type
}

type GenBridgePair struct {
	Forward GenBridge
	Inverse GenBridge
}

type GenBridge struct {
	StructField        string
	Name               string
	LeftNode           *gen.Type
	RightNode          *gen.Type
	LowerName          string
	LowerLeftNodeName  string
	LowerRightNodeName string
	RelName            string
	RelType            string
	LeftField          string
	RightField         string
	PivotTable         string
	PivotLeftField     string
	PivotRightField    string
}

func (ext *Extension) prepareGenGraph(g *gen.Graph) (*GenGraph, error) {
	graph := &GenGraph{EntGraph: g}
	graph.Nodes = ext.buildGenNodes(g.Nodes)
	graph.BridgePairs = ext.buildBridgePairs(graph.Nodes)
	return graph, nil
}

func (ext *Extension) buildGenNodes(nodes []*gen.Type) []GenNode {
	var result []GenNode
	for _, node := range nodes {
		if !ext.IsNodeInclude(node) {
			continue
		}
		result = append(result, ext.mapToGenNode(node))
	}
	return result
}

func (ext *Extension) mapToGenNode(node *gen.Type) GenNode {
	var cols []*gen.Field
	for _, f := range node.Fields {
		if ext.IsFieldInclude(node, f) {
			cols = append(cols, f)
		}
	}
	var pks []*gen.Field
	if f := node.ID; f != nil {
		pks = append(pks, f)
		cols = append(cols, f)
	}
	pks = append(pks, node.EdgeSchema.ID...)

	return GenNode{
		HasPolicy:     node.NumPolicy() > 0,
		EntNode:       node,
		NodeName:      node.Name,
		LowerNodeName: lowerFirst(node.Name),
		TableName:     node.Table(),
		Columns:       cols,
		PKs:           pks,
	}
}

func (ext *Extension) buildBridgePairs(genNodes []GenNode) []GenBridgePair {
	var pairs []GenBridgePair
	for _, gn := range genNodes {
		node := gn.EntNode
		for _, e := range node.Edges {
			if e.Owner != node || e.Ref == nil || !ext.IsNodeInclude(e.Type) {
				continue
			}
			forward := makeForwardGenBridge(e)
			inverse := makeInverseGenBridge(forward, e)
			pairs = append(pairs, GenBridgePair{forward, inverse})
		}
	}
	return pairs
}

func makeInverseGenBridge(forward GenBridge, e *gen.Edge) GenBridge {
	var relType gen.Rel
	switch v := e.Rel.Type; v {
	case gen.M2M, gen.O2O:
		relType = v
	case gen.O2M:
		relType = gen.M2O
	case gen.M2O:
		relType = gen.O2M
	}

	structField := e.Ref.StructField()
	inverse := GenBridge{
		StructField:     structField,
		Name:            fmt.Sprintf("%s%sBridge", e.Type.Name, structField),
		LeftNode:        e.Type,
		RightNode:       e.Owner,
		RelName:         e.Ref.Name,
		LeftField:       forward.RightField,
		RightField:      forward.LeftField,
		PivotTable:      forward.PivotTable,
		PivotLeftField:  forward.PivotRightField,
		PivotRightField: forward.PivotLeftField,
		RelType:         relType.String(),
	}
	inverse.LowerName = lowerFirst(inverse.Name)
	inverse.LowerLeftNodeName = lowerFirst(inverse.LeftNode.Name)
	inverse.LowerRightNodeName = lowerFirst(inverse.RightNode.Name)

	return inverse
}

func makeForwardGenBridge(e *gen.Edge) GenBridge {
	structField := e.StructField()
	name := fmt.Sprintf("%s%sBridge", e.Owner.Name, structField)
	b := GenBridge{
		StructField:        structField,
		Name:               name,
		LeftNode:           e.Owner,
		RightNode:          e.Type,
		LowerName:          lowerFirst(name),
		LowerLeftNodeName:  lowerFirst(e.Owner.Name),
		LowerRightNodeName: lowerFirst(e.Type.Name),
		RelName:            e.Name,
		RelType:            e.Rel.Type.String(),
	}
	// assign fields
	if e.Owner.ID != nil {
		b.LeftField = e.Owner.ID.StorageKey()
	}
	switch e.Rel.Type {
	case gen.M2M:
		b.PivotTable = e.Rel.Table
		if len(e.Rel.Columns) >= 2 {
			b.PivotLeftField = e.Rel.Columns[0]
			b.PivotRightField = e.Rel.Columns[1]
		}
		if e.Type.ID != nil {
			b.RightField = e.Type.ID.StorageKey()
		}
	case gen.O2M, gen.M2O, gen.O2O:
		b.RightField = e.Rel.Column()
	}
	return b
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func debug(v any) string {
	fmt.Printf("DEBUG: %#v\n", v)
	return ""
}

func isGenType(x any) bool {
	_, ok := x.(*gen.Type)
	return ok
}
