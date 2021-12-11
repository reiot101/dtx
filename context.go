package dtx

import (
	"sync"

	"github.com/reiot777/dtx/packet"
	"google.golang.org/protobuf/proto"
)

type Context struct {
	id    string
	tx    Tx
	msg   proto.Message
	state packet.StateEnum_State
	sync.Mutex
}

func (c *Context) Message() proto.Message {
	return c.msg
}

func (c *Context) IsAbout() bool {
	switch c.state {
	case packet.StateEnum_ABOUT, packet.StateEnum_ROLLBACK:
		return true
	default:
		return false
	}
}

func (c *Context) End() {
	c.Lock()
	c.state = packet.StateEnum_END
	c.Unlock()
}
