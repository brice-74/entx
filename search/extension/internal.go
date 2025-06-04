//go:build examples
// +build examples

package extension

// SetImportPath configures a custom import path for use in examples
// and local development only. This file is excluded from normal builds.
//
// To enable this in VSCode, add to your workspace settings:
//
//	{
//	    "gopls": {
//	        "buildFlags": ["-tags=examples"]
//	    }
//	}
func SetImportPath(path string) Option {
	return func(c *Config) {
		c.importPath = path
	}
}
