package dtx

import (
	"container/list"
	"context"
	"log"
	"os"
	"time"

	"github.com/reiot777/dtx/internal"
	"github.com/reiot777/dtx/packet"
	"github.com/reiot777/dtx/stores"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Codec int8

const (
	JSON Codec = iota
	PROTO
)

var _ Tx = (*tx)(nil)

// Tx is pipeline transaction interface.
type Tx interface {
	Begin() Tx
	Exec(Task) Tx
	End()
	About(error)
	Rollback(tlog *packet.TxLog) error
}

type Options struct {
	debug bool
	codec Codec
}

type Option func(o *Options)

func Debug() Option {
	return func(o *Options) {
		o.debug = true
	}
}

func SetCodec(c Codec) Option {
	return func(o *Options) {
		o.codec = c
	}
}

// New is create transcation pipeline instance.
func New(ctx context.Context, id string, opts ...Option) Tx {
	// default
	tx := &tx{
		ctx: &Context{
			id:    id,
			state: packet.StateEnum_UNKNOWN,
		},
		opts: &Options{
			debug: false,
			codec: JSON,
		},
		now:   time.Now,
		tasks: make(tasks),
		log:   log.New(os.Stdout, "[dtx] ", log.LstdFlags),
		stack: list.New(),
	}

	// apply options
	for i := range opts {
		opts[i](tx.opts)
	}

	// set tx in context
	tx.ctx.tx = tx

	return tx
}

// tx is implemented Tx interface.
type tx struct {
	ctx   *Context
	opts  *Options
	now   func() time.Time
	tasks tasks
	log   *log.Logger
	stack *list.List
	store stores.Store
}

func (tx *tx) Begin() Tx {
	newLog := &packet.TxLog{
		Id:        tx.ctx.id,
		State:     packet.StateEnum_BEGIN,
		Timestamp: tx.now().Unix(),
	}

	tx.stack.PushBack(newLog)

	if err := tx.store.Append(newLog); err != nil {
		tx.log.Panicln(packet.StateEnum_BEGIN, err)
	}

	tx.debugPrint(newLog)

	return tx
}

func (tx *tx) Exec(task Task) Tx {
	if tx.ctx.state == packet.StateEnum_END {
		return tx
	}

	tx.tasks.Put(task)

	newLog := &packet.TxLog{
		Id:        tx.ctx.id,
		State:     packet.StateEnum_COMMIT,
		Task:      task.ID,
		Type:      internal.ParseType(task.CommitMsg).String(),
		Data:      tx.marshal(task.CommitMsg),
		Timestamp: tx.now().Unix(),
	}

	tx.stack.PushBack(newLog)

	// set current msg, state in context
	tx.ctx.msg = task.CommitMsg
	tx.ctx.state = newLog.State

	if err := task.Handler.Commit(tx.ctx); err != nil {
		tx.About(err)
		return tx
	}

	if err := tx.store.Append(newLog); err != nil {
		tx.log.Panicln(packet.StateEnum_COMMIT, err)
	}

	tx.debugPrint(newLog)

	return tx
}

func (tx *tx) End() {
	newLog := &packet.TxLog{
		Id:        tx.ctx.id,
		State:     packet.StateEnum_END,
		Timestamp: tx.now().Unix(),
	}

	tx.stack.PushBack(newLog)

	if err := tx.store.Append(newLog); err != nil {
		tx.log.Panicln(packet.StateEnum_END, err)
	}

	tx.debugPrint(newLog)

	tx.stack = tx.stack.Init()
}

func (tx *tx) About(e error) {
	logs, err := tx.store.Load(tx.ctx.id)
	if err != nil {
		tx.log.Panicln(packet.StateEnum_ABOUT, err)
	}

	var last *packet.TxLog
	if v := tx.stack.Remove(tx.stack.Back()); v == nil {
		return
	} else {
		last = v.(*packet.TxLog)
		last.State = packet.StateEnum_ABOUT
		last.Timestamp = tx.now().Unix()
		last.ErrorMessage = e.Error()

		tx.stack.PushBack(last)
	}

	if err := tx.store.Append(last); err != nil {
		tx.log.Panicln(packet.StateEnum_ABOUT, err)
	}

	tx.debugPrint(last)

	// replay logs and rollback
	tx.replay(logs)
}

func (tx *tx) Rollback(tlog *packet.TxLog) error {
	task, ok := tx.tasks.Get(tlog.Task)
	if !ok {
		return nil
	}

	newLog := &packet.TxLog{
		Id:        tx.ctx.id,
		State:     packet.StateEnum_ROLLBACK,
		Task:      task.ID,
		Type:      internal.ParseType(task.RollbackMsg).String(),
		Data:      tx.marshal(task.RollbackMsg),
		Timestamp: tx.now().Unix(),
	}

	tx.stack.PushBack(newLog)

	// replace current msg, state in context
	tx.ctx.msg = task.RollbackMsg
	tx.ctx.state = newLog.State

	if err := task.Handler.Rollback(tx.ctx); err != nil {
		tx.About(err)
	}

	if err := tx.store.Append(newLog); err != nil {
		return err
	}

	tx.debugPrint(newLog)

	return nil
}

// replay tx logs
func (tx *tx) replay(logs []*packet.TxLog) {
	for i := len(logs) - 1; i >= 0; i-- {
		if logs[i].State == packet.StateEnum_COMMIT {
			if err := tx.Rollback(logs[i]); err != nil {
				tx.log.Panicln(err)
			}
		}
	}
}

// debugPrint debug print
func (tx *tx) debugPrint(tlog *packet.TxLog) {
	if !tx.opts.debug {
		return
	}
	tx.log.Println(tlog)
}

// marshal proto message to binary or json
func (tx *tx) marshal(msg proto.Message) []byte {
	var (
		b   []byte
		err error
	)

	switch tx.opts.codec {
	case JSON:
		b, err = protojson.Marshal(msg)
	case PROTO:
		b, err = proto.Marshal(msg)
	}

	if err != nil {
		panic("failed marshal proto message")
	}

	return b
}
