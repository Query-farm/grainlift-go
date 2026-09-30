// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Backend opens independently owned connections. Options already include
// authoritative server configuration. Context cancellation is cooperative.
type Backend interface {
	Open(context.Context, string, OpenConnectionRequest) (Connection, error)
}

// QueryResult transfers Reader ownership to Service. Its schema remains stable.
// Build a result whose state travels in continuation tokens with
// NewProducerResult, which also sets Producer.
type QueryResult struct {
	Reader       array.RecordReader
	RowsAffected *int64
	// Producer, when set, is the serializable initial state behind Reader.
	// Service resumes it from continuation tokens instead of keeping Reader.
	Producer ResultProducer
}

// PartitionedResult contains backend opaque descriptors, wrapped by Service
// with an authenticated principal-bound expiring token before leaving the server.
type PartitionedResult struct {
	Schema       *arrow.Schema
	RowsAffected int64
	Partitions   [][]byte
}

// Connection defines the complete backend surface. Embed UnimplementedConnection
// and override supported capabilities. Close must release owned resources.
// Cancel may run concurrently and must be thread-safe and nonblocking.
type Connection interface {
	NewStatement(ctx context.Context) (Statement, error)
	SetOption(ctx context.Context, key string, value OptionValue) error
	GetOption(ctx context.Context, key, kind string) (OptionValue, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Cancel(ctx context.Context) error
	GetInfo(ctx context.Context, request GetInfoRequest) (*QueryResult, error)
	GetObjects(ctx context.Context, request GetObjectsRequest) (*QueryResult, error)
	GetTableSchema(ctx context.Context, request GetTableSchemaRequest) (*arrow.Schema, error)
	GetTableTypes(ctx context.Context) (*QueryResult, error)
	GetStatisticNames(ctx context.Context) (*QueryResult, error)
	GetStatistics(ctx context.Context, request GetStatisticsRequest) (*QueryResult, error)
	ReadPartition(ctx context.Context, descriptor []byte) (*QueryResult, error)
	Close() error
}

// UnimplementedConnection returns explicit ADBC NOT_IMPLEMENTED for optional features.
type UnimplementedConnection struct{}

func (UnimplementedConnection) NewStatement(ctx context.Context) (Statement, error) {
	return nil, unsupported()
}
func (UnimplementedConnection) SetOption(ctx context.Context, key string, value OptionValue) error {
	return unsupported()
}
func (UnimplementedConnection) GetOption(ctx context.Context, key, kind string) (OptionValue, error) {
	return OptionValue{}, unsupported()
}
func (UnimplementedConnection) Commit(ctx context.Context) error   { return unsupported() }
func (UnimplementedConnection) Rollback(ctx context.Context) error { return unsupported() }
func (UnimplementedConnection) Cancel(ctx context.Context) error   { return unsupported() }
func (UnimplementedConnection) GetInfo(ctx context.Context, request GetInfoRequest) (*QueryResult, error) {
	return nil, unsupported()
}
func (UnimplementedConnection) GetObjects(ctx context.Context, request GetObjectsRequest) (*QueryResult, error) {
	return nil, unsupported()
}
func (UnimplementedConnection) GetTableSchema(ctx context.Context, request GetTableSchemaRequest) (*arrow.Schema, error) {
	return nil, unsupported()
}
func (UnimplementedConnection) GetTableTypes(ctx context.Context) (*QueryResult, error) {
	return nil, unsupported()
}
func (UnimplementedConnection) GetStatisticNames(ctx context.Context) (*QueryResult, error) {
	return nil, unsupported()
}
func (UnimplementedConnection) GetStatistics(ctx context.Context, request GetStatisticsRequest) (*QueryResult, error) {
	return nil, unsupported()
}
func (UnimplementedConnection) ReadPartition(ctx context.Context, descriptor []byte) (*QueryResult, error) {
	return nil, unsupported()
}
func (UnimplementedConnection) Close() error { return nil }

// Statement defines the complete backend surface. Embed UnimplementedStatement
// and override supported capabilities. Close must release owned resources.
// Cancel may run concurrently and must be thread-safe and nonblocking.
type Statement interface {
	SetSQLQuery(ctx context.Context, sql string) error
	SetSubstraitPlan(ctx context.Context, plan []byte) error
	Prepare(ctx context.Context) error
	Bind(ctx context.Context, batch arrow.RecordBatch) error
	BindStream(ctx context.Context, reader array.RecordReader) error
	Execute(ctx context.Context) (*QueryResult, error)
	ExecuteUpdate(ctx context.Context) (*int64, error)
	ExecuteSchema(ctx context.Context) (*arrow.Schema, error)
	GetParameterSchema(ctx context.Context) (*arrow.Schema, error)
	ExecutePartitions(ctx context.Context) (*PartitionedResult, error)
	SetOption(ctx context.Context, key string, value OptionValue) error
	GetOption(ctx context.Context, key, kind string) (OptionValue, error)
	Cancel(ctx context.Context) error
	Close() error
}

// UnimplementedStatement returns explicit ADBC NOT_IMPLEMENTED for optional features.
type UnimplementedStatement struct{}

func (UnimplementedStatement) SetSQLQuery(ctx context.Context, sql string) error {
	return unsupported()
}
func (UnimplementedStatement) SetSubstraitPlan(ctx context.Context, plan []byte) error {
	return unsupported()
}
func (UnimplementedStatement) Prepare(ctx context.Context) error { return unsupported() }
func (UnimplementedStatement) Bind(ctx context.Context, batch arrow.RecordBatch) error {
	return unsupported()
}
func (UnimplementedStatement) BindStream(ctx context.Context, reader array.RecordReader) error {
	return unsupported()
}
func (UnimplementedStatement) Execute(ctx context.Context) (*QueryResult, error) {
	return nil, unsupported()
}
func (UnimplementedStatement) ExecuteUpdate(ctx context.Context) (*int64, error) {
	return nil, unsupported()
}
func (UnimplementedStatement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, unsupported()
}
func (UnimplementedStatement) GetParameterSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, unsupported()
}
func (UnimplementedStatement) ExecutePartitions(ctx context.Context) (*PartitionedResult, error) {
	return nil, unsupported()
}
func (UnimplementedStatement) SetOption(ctx context.Context, key string, value OptionValue) error {
	return unsupported()
}
func (UnimplementedStatement) GetOption(ctx context.Context, key, kind string) (OptionValue, error) {
	return OptionValue{}, unsupported()
}
func (UnimplementedStatement) Cancel(ctx context.Context) error { return unsupported() }
func (UnimplementedStatement) Close() error                     { return nil }
