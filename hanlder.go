package dtx

import "google.golang.org/protobuf/proto"

type Handler interface {
	Commit(c *Context) error
	Rollback(c *Context) error
}

type HandleFunc func(*Context) error

type Task struct {
	ID                     string
	CommitMsg, RollbackMsg proto.Message
	Handler                Handler
	HandleFunc             HandleFunc
}

type tasks map[string]Task

func (t tasks) Put(task Task) {
	t[task.ID] = task
}
func (t tasks) Get(k string) (Task, bool) {
	task, ok := t[k]
	return task, ok
}
