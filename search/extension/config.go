package extension

import (
	"strings"

	"entgo.io/ent/entc/gen"
)

const (
	searchableNodeAnnotKey = "entx_searchable_node"
	entxImportPath         = "github.com/brice-74/entx"
)

type Annotation struct{ NodeConfig }

func (Annotation) Name() string { return searchableNodeAnnotKey }

func ExcludeNode() Annotation { return Annotation{NodeConfig{Included: false}} }
func IncludeNode(mods ...NodeOption) Annotation {
	cfg := NodeConfig{Included: true}
	for _, fn := range mods {
		fn(&cfg)
	}
	return Annotation{cfg}
}

type computedNodes map[string]map[string]bool

func (c computedNodes) IsNodeInclude(node *gen.Type) bool {
	return c[node.Name]["__node"]
}

func (c computedNodes) IsFieldInclude(node *gen.Type, field *gen.Field) bool {
	if n := c[node.Name]; !n[field.Name] {
		return n[field.StorageKey()]
	}
	return true
}

type (
	Option     func(*Config)
	NodeOption func(*NodeConfig)
)

type NodeConfig struct {
	Included      bool
	IncludeFields []string
	ExcludeFields []string
}

type Config struct {
	importName       string
	importPath       string
	IncludeAllNodes  bool
	IncludeAllFields bool
	Nodes            map[string]*NodeConfig
}

func NewConfig(opts ...Option) *Config {
	cfg := &Config{
		importPath:       entxImportPath,
		IncludeAllNodes:  true,
		IncludeAllFields: true,
		Nodes:            make(map[string]*NodeConfig),
	}
	for _, o := range opts {
		o(cfg)
	}
	cfg.importName = getLastPathSegment(cfg.importPath)
	return cfg
}

func (c *Config) ComputeNodes(nodes ...*gen.Type) computedNodes {
	res := make(computedNodes, len(nodes))
	for _, t := range nodes {
		included := c.isIncluded(t)

		var inner map[string]bool
		if included {
			inner = c.computeFieldsInclusion(t)
		} else {
			inner = make(map[string]bool, 1)
		}

		inner["__node"] = included
		res[t.Name] = inner
	}
	return res
}

func (c *Config) isIncluded(t *gen.Type) bool {
	included := c.IncludeAllNodes
	if nc, ok := c.Nodes[t.Name]; ok && nc != nil {
		included = nc.Included
	}
	if a, ok := t.Annotations[searchableNodeAnnotKey].(map[string]any); ok {
		included = a["Included"].(bool)
	}
	return included
}

func (c *Config) computeFieldsInclusion(t *gen.Type) map[string]bool {
	res := make(map[string]bool, len(t.Fields))

	var incMap, excMap map[string]struct{}
	if nc := c.Nodes[t.Name]; nc != nil {
		incMap = getSet(nc.IncludeFields)
		excMap = getSet(nc.ExcludeFields)
	} else {
		incMap = make(map[string]struct{}, 0)
		excMap = make(map[string]struct{}, 0)
	}

	for _, f := range t.Fields {
		allowed := c.IncludeAllFields
		if _, ok := incMap[f.Name]; ok {
			allowed = true
		}
		if _, ok := excMap[f.Name]; ok {
			allowed = false
		}
		if a, ok := f.Annotations[searchableNodeAnnotKey].(map[string]any); ok {
			allowed = a["Included"].(bool)
		}
		res[f.Name] = allowed
	}
	return res
}

func getSet(items []string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, s := range items {
		m[s] = struct{}{}
	}
	return m
}

func IncludeFields(fields ...string) NodeOption {
	return func(n *NodeConfig) { n.IncludeFields = append(n.IncludeFields, fields...) }
}
func ExcludeFields(fields ...string) NodeOption {
	return func(n *NodeConfig) { n.ExcludeFields = append(n.ExcludeFields, fields...) }
}

func GlobalIncludeNodes() Option  { return func(c *Config) { c.IncludeAllNodes = true } }
func GlobalExcludeNodes() Option  { return func(c *Config) { c.IncludeAllNodes = false } }
func GlobalIncludeFields() Option { return func(c *Config) { c.IncludeAllFields = true } }
func GlobalExcludeFields() Option { return func(c *Config) { c.IncludeAllFields = false } }

func SetNodesInclusion(included bool, names ...string) Option {
	return func(c *Config) {
		for _, name := range names {
			nc := c.Nodes[name]
			if nc == nil {
				nc = &NodeConfig{}
				c.Nodes[name] = nc
			}
			nc.Included = included
		}
	}
}
func IncludeNodes(names ...string) Option { return SetNodesInclusion(true, names...) }
func ExcludeNodes(names ...string) Option { return SetNodesInclusion(false, names...) }

func IncludeNodeWith(name string, opts ...NodeOption) Option {
	return func(c *Config) {
		SetNodesInclusion(true, name)(c)
		nc := c.Nodes[name]
		for _, o := range opts {
			o(nc)
		}
	}
}

func getLastPathSegment(path string) string {
	lastSlash := strings.LastIndex(path, "/")
	if lastSlash == -1 {
		return path
	}
	return path[lastSlash+1:]
}
