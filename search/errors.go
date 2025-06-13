package search

import "fmt"

type ValidationError struct {
	Rule string
	Err  error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed on %s: %v", e.Rule, e.Err)
}
func (e *ValidationError) Unwrap() error { return e.Err }

type QueryBuildError struct {
	Op  string
	Err error
}

func (e *QueryBuildError) Error() string {
	return fmt.Sprintf("build failed at %s: %v", e.Op, e.Err)
}
func (e *QueryBuildError) Unwrap() error { return e.Err }

type ExecError struct {
	Op  string
	Err error
}

func (e *ExecError) Error() string {
	return fmt.Sprintf("execution failed at %s: %v", e.Op, e.Err)
}
func (e *ExecError) Unwrap() error { return e.Err }
