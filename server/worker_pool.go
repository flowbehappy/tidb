package server

import (
	"context"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/logutil"
)

type ReadPacketTaskRes struct {
	data []byte
	err  error
}

type ReadPacketTask struct {
	conn *clientConn
	ctx  *context.Context

	resChan chan ReadPacketTaskRes
}

type ExecuteStmtTaskRes struct {
	rs  ResultSet
	err error
}

type ExecuteStmtTask struct {
	conn *clientConn
	ctx  *context.Context
	stmt *ast.ExecuteStmt

	resChan chan ExecuteStmtTaskRes
}

type WriteChunksTaskRes struct {
	retryable bool
	err       error
}

type WriteChunksTask struct {
	conn         *clientConn
	ctx          *context.Context
	rs           *ResultSet
	binary       bool
	serverStatus uint16

	resChan chan WriteChunksTaskRes
}

type FlushTaskRes struct {
	err error
}

type FlushTask struct {
	conn *clientConn
	ctx  *context.Context

	resChan chan FlushTaskRes
}

type WorkerPoolConfig struct {
	ReadPacketWokerCount  uint32
	ExecuteStmtWokerCount uint32
	WriteChunksWokerCount uint32
	FlushWokerCount       uint32
}

type WorkerPool struct {
	readPacketChan  chan ReadPacketTask
	executeStmtChan chan ExecuteStmtTask
	writeChunksChan chan WriteChunksTask
	flushChan       chan FlushTask
}

func (wp *WorkerPool) Start(config WorkerPoolConfig) {

	logutil.BgLogger().Info("WorkerPool Start")

	wp.readPacketChan = make(chan ReadPacketTask, 120)
	wp.executeStmtChan = make(chan ExecuteStmtTask, 120)
	wp.writeChunksChan = make(chan WriteChunksTask, 120)
	wp.flushChan = make(chan FlushTask, 120)

	for i := 0; i < int(config.ReadPacketWokerCount); i++ {
		go wp.doReadPacket()
	}
	for i := 0; i < int(config.ExecuteStmtWokerCount); i++ {
		go wp.doExecuteStmt()
	}
	for i := 0; i < int(config.WriteChunksWokerCount); i++ {
		go wp.doWriteChunks()
	}
	for i := 0; i < int(config.FlushWokerCount); i++ {
		go wp.doFlush()
	}
}

func (wp *WorkerPool) Shutdown() {
	close(wp.readPacketChan)
	close(wp.executeStmtChan)
	close(wp.writeChunksChan)
	close(wp.flushChan)
}

func (wp *WorkerPool) doReadPacket() {
	for task := range wp.readPacketChan {
		data, err := task.conn.readPacket()
		task.resChan <- ReadPacketTaskRes{data, err}
	}
}

func (wp *WorkerPool) ReadPacket(conn *clientConn, ctx context.Context) ([]byte, error) {
	return conn.readPacket()

	// task := ReadPacketTask{
	// 	conn:    conn,
	// 	ctx:     &ctx,
	// 	resChan: make(chan ReadPacketTaskRes, 1),
	// }

	// wp.readPacketChan <- task
	// res := <-task.resChan

	// return res.data, res.err
}

func (wp *WorkerPool) doExecuteStmt() {
	for task := range wp.executeStmtChan {
		rs, err := (&task.conn.ctx).ExecuteStmt(*task.ctx, task.stmt)
		task.resChan <- ExecuteStmtTaskRes{rs, err}
	}
}

func (wp *WorkerPool) ExecuteStmt(conn *clientConn, ctx context.Context, stmt *ast.ExecuteStmt) (ResultSet, error) {
	return (&conn.ctx).ExecuteStmt(ctx, stmt)

	// task := ExecuteStmtTask{
	// 	conn:    conn,
	// 	ctx:     &ctx,
	// 	stmt:    stmt,
	// 	resChan: make(chan ExecuteStmtTaskRes, 1),
	// }

	// wp.executeStmtChan <- task
	// res := <-task.resChan
	// return res.rs, res.err
}

func (wp *WorkerPool) doWriteChunks() {
	for task := range wp.writeChunksChan {
		retryable, err := task.conn.writeChunks(*task.ctx, *task.rs, task.binary, task.serverStatus)
		task.resChan <- WriteChunksTaskRes{retryable, err}
	}
}

func (wp *WorkerPool) WriteChunks(conn *clientConn,
	ctx context.Context,
	rs ResultSet,
	binary bool,
	serverStatus uint16) (bool, error) {

	return conn.writeChunks(ctx, rs, binary, serverStatus)

	// task := WriteChunksTask{
	// 	conn:         conn,
	// 	ctx:          &ctx,
	// 	rs:           &rs,
	// 	binary:       binary,
	// 	serverStatus: serverStatus,
	// 	resChan:      make(chan WriteChunksTaskRes, 1),
	// }

	// wp.writeChunksChan <- task
	// res := <-task.resChan
	// return res.retryable, res.err
}

func (wp *WorkerPool) doFlush() {
	for task := range wp.flushChan {
		err := task.conn.flush(*task.ctx)
		task.resChan <- FlushTaskRes{err: err}
	}
}

func (wp *WorkerPool) Flush(conn *clientConn, ctx context.Context) error {
	return conn.flush(ctx)
	// task := FlushTask{
	// 	conn:    conn,
	// 	ctx:     &ctx,
	// 	resChan: make(chan FlushTaskRes, 1),
	// }

	// wp.flushChan <- task
	// res := <-task.resChan
	// return res.err
}
