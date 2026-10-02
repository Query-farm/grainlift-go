// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0
package grainlift

import (
	"context"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

type sessionParams struct {
	SessionID string `vgirpc:"session_id"`
}
type statementParams struct {
	SessionID   string `vgirpc:"session_id"`
	StatementID string `vgirpc:"statement_id"`
}
type resultParams struct {
	SessionID string `vgirpc:"session_id"`
	ResultID  string `vgirpc:"result_id"`
}
type readParams struct {
	SessionID string `vgirpc:"session_id"`
	ResultID  string `vgirpc:"result_id"`
	Sequence  int64  `vgirpc:"sequence"`
}
type sqlParams struct {
	SessionID   string `vgirpc:"session_id"`
	StatementID string `vgirpc:"statement_id"`
	SQL         string `vgirpc:"sql"`
}
type planParams struct {
	SessionID   string `vgirpc:"session_id"`
	StatementID string `vgirpc:"statement_id"`
	Payload     []byte `vgirpc:"payload"`
}
type bindParams struct {
	SessionID   string `vgirpc:"session_id"`
	StatementID string `vgirpc:"statement_id"`
	SchemaIPC   []byte `vgirpc:"schema_ipc"`
}
type partitionParams struct {
	SessionID string `vgirpc:"session_id"`
	Payload   []byte `vgirpc:"payload"`
}
type connectionOptionParams struct {
	SessionID string `vgirpc:"session_id"`
	Key       string `vgirpc:"key"`
	ValueType string `vgirpc:"value_type"`
}
type statementOptionParams struct {
	SessionID   string `vgirpc:"session_id"`
	StatementID string `vgirpc:"statement_id"`
	Key         string `vgirpc:"key"`
	ValueType   string `vgirpc:"value_type"`
}
type requestParams[T any] struct {
	Request T `vgirpc:"request"`
}

func runSession[R any](s *Service, ctx context.Context, call *vgirpc.CallContext, id string, f func(*session) (R, error)) (R, error) {
	var zero R
	ss, u, e := s.acquire(ctx, call, id)
	if e != nil {
		return zero, e
	}
	defer u()
	r, e := f(ss)
	return r, backendError(e)
}
func runStatement[R any](s *Service, ctx context.Context, call *vgirpc.CallContext, sid, id string, f func(*session, *statement) (R, error)) (R, error) {
	return runSession(s, ctx, call, sid, func(ss *session) (R, error) {
		st := ss.statements[id]
		if st == nil {
			var z R
			return z, failure("not_found", "Unknown statement")
		}
		return f(ss, st)
	})
}
func ok(e error) (OkResponse, error) { return OkResponse{e == nil}, e }
func (s *Service) register(rpc *vgirpc.Server) {
	namedUnary(rpc, "open_connection", func(ctx context.Context, c *vgirpc.CallContext, p requestParams[OpenConnectionRequest]) (SessionResponse, error) {
		return s.open(ctx, c, p.Request)
	})
	vgirpc.Unary(rpc, "close_connection", func(ctx context.Context, c *vgirpc.CallContext, p sessionParams) (OkResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (OkResponse, error) { return ok(s.closeSession(p.SessionID, ss)) })
	})
	vgirpc.Unary(rpc, "new_statement", func(ctx context.Context, c *vgirpc.CallContext, p sessionParams) (StatementResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (StatementResponse, error) {
			if len(ss.statements) >= s.limits.Statements {
				return StatementResponse{}, failure("invalid_state", "Statement limit reached")
			}
			st, e := ss.conn.NewStatement(ctx)
			if e != nil {
				return StatementResponse{}, e
			}
			if st == nil {
				return StatementResponse{}, failure("invalid_data", "Missing statement")
			}
			id := identifier()
			ss.guard.Lock()
			ss.statements[id] = &statement{backend: st}
			ss.guard.Unlock()
			return StatementResponse{p.SessionID, id}, nil
		})
	})
	vgirpc.Unary(rpc, "close_statement", func(ctx context.Context, c *vgirpc.CallContext, p statementParams) (OkResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (OkResponse, error) {
			s.closeResult(ss, st.resultID)
			ss.guard.Lock()
			defer ss.guard.Unlock()
			e := st.backend.Close()
			clearBinding(st)
			delete(ss.statements, p.StatementID)
			return ok(e)
		})
	})
	vgirpc.Unary(rpc, "set_sql_query", func(ctx context.Context, c *vgirpc.CallContext, p sqlParams) (OkResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (OkResponse, error) {
			if len(p.SQL) > s.limits.SQLBytes || !validText(p.SQL) {
				return ok(failure("invalid_arguments", "Invalid SQL size or text"))
			}
			s.closeResult(ss, st.resultID)
			e := st.backend.SetSQLQuery(ctx, p.SQL)
			if e == nil {
				clearBinding(st)
			}
			return ok(e)
		})
	})
	vgirpc.Unary(rpc, "set_substrait_plan", func(ctx context.Context, c *vgirpc.CallContext, p planParams) (OkResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (OkResponse, error) {
			if len(p.Payload) > s.limits.RequestBytes {
				return ok(failure("invalid_arguments", "Plan limit exceeded"))
			}
			s.closeResult(ss, st.resultID)
			e := st.backend.SetSubstraitPlan(ctx, p.Payload)
			if e == nil {
				clearBinding(st)
			}
			return ok(e)
		})
	})
	vgirpc.Unary(rpc, "execute", func(ctx context.Context, c *vgirpc.CallContext, p statementParams) (ExecuteResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (ExecuteResponse, error) {
			if st.binding != nil && !st.binding.finished {
				return ExecuteResponse{}, failure("invalid_state", "Binding upload is incomplete")
			}
			s.closeResult(ss, st.resultID)
			q, e := st.backend.Execute(ctx)
			if e != nil {
				return ExecuteResponse{}, e
			}
			return s.addResult(ss, p.StatementID, q)
		})
	})
	vgirpc.Unary(rpc, "execute_update", func(ctx context.Context, c *vgirpc.CallContext, p statementParams) (UpdateResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (UpdateResponse, error) {
			if st.binding != nil && !st.binding.finished {
				return UpdateResponse{}, failure("invalid_state", "Binding upload is incomplete")
			}
			s.closeResult(ss, st.resultID)
			n, e := st.backend.ExecuteUpdate(ctx)
			if n != nil && *n < -1 {
				return UpdateResponse{}, failure("invalid_data", "Invalid affected row count")
			}
			return UpdateResponse{n}, e
		})
	})
	vgirpc.Unary(rpc, "close_result", func(ctx context.Context, c *vgirpc.CallContext, p resultParams) (OkResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (OkResponse, error) { s.closeResult(ss, p.ResultID); return ok(nil) })
	})
	vgirpc.DynamicProducerWithHeader(rpc, "read_result", nil, func(ctx context.Context, c *vgirpc.CallContext, p readParams) (*vgirpc.StreamResult, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (*vgirpc.StreamResult, error) {
			r := ss.results[p.ResultID]
			if r == nil {
				return nil, failure("not_found", "Unknown result")
			}
			if r.producer != nil {
				if p.Sequence != 0 {
					return nil, failure("invalid_arguments", "Producer results resume from continuation tokens")
				}
				return &vgirpc.StreamResult{OutputSchema: r.schema, State: &ResultCursor{p.SessionID, p.ResultID, 0, r.producer}}, nil
			}
			if p.Sequence < 0 || p.Sequence > r.sequence || p.Sequence < r.sequence-1 {
				return nil, failure("invalid_state", "Invalid result sequence")
			}
			var schema *arrow.Schema
			if r.query.Reader != nil {
				schema = r.query.Reader.Schema()
			} else if r.last != nil {
				schema = r.last.Schema()
			} else {
				schema = r.schema
			}
			return &vgirpc.StreamResult{OutputSchema: schema, State: &ResultCursor{p.SessionID, p.ResultID, p.Sequence, nil}}, nil
		})
	})
	vgirpc.Unary(rpc, "commit", func(ctx context.Context, c *vgirpc.CallContext, p sessionParams) (OkResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (OkResponse, error) { return ok(ss.conn.Commit(ctx)) })
	})
	vgirpc.Unary(rpc, "rollback", func(ctx context.Context, c *vgirpc.CallContext, p sessionParams) (OkResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (OkResponse, error) { return ok(ss.conn.Rollback(ctx)) })
	})
	vgirpc.Unary(rpc, "cancel_connection", func(ctx context.Context, c *vgirpc.CallContext, p sessionParams) (OkResponse, error) {
		return s.cancel(ctx, c, p.SessionID, "")
	})
	vgirpc.Unary(rpc, "prepare", func(ctx context.Context, c *vgirpc.CallContext, p statementParams) (OkResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (OkResponse, error) { return ok(st.backend.Prepare(ctx)) })
	})
	vgirpc.Unary(rpc, "cancel_statement", func(ctx context.Context, c *vgirpc.CallContext, p statementParams) (OkResponse, error) {
		return s.cancel(ctx, c, p.SessionID, p.StatementID)
	})
	vgirpc.Unary(rpc, "execute_schema", func(ctx context.Context, c *vgirpc.CallContext, p statementParams) (SchemaResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (SchemaResponse, error) {
			s.closeResult(ss, st.resultID)
			schema, e := st.backend.ExecuteSchema(ctx)
			if e != nil {
				return SchemaResponse{}, e
			}
			return s.schemaResponse(schema)
		})
	})
	vgirpc.Unary(rpc, "get_parameter_schema", func(ctx context.Context, c *vgirpc.CallContext, p statementParams) (SchemaResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (SchemaResponse, error) {
			schema, e := st.backend.GetParameterSchema(ctx)
			if e != nil {
				return SchemaResponse{}, e
			}
			return s.schemaResponse(schema)
		})
	})
	namedUnary(rpc, "get_info", func(ctx context.Context, c *vgirpc.CallContext, p requestParams[GetInfoRequest]) (ExecuteResponse, error) {
		return runSession(s, ctx, c, p.Request.SessionID, func(ss *session) (ExecuteResponse, error) {
			if e := validateRequest(p.Request); e != nil {
				return ExecuteResponse{}, e
			}
			q, e := ss.conn.GetInfo(ctx, p.Request)
			if e != nil {
				return ExecuteResponse{}, e
			}
			return s.addResult(ss, "", q)
		})
	})
	namedUnary(rpc, "get_objects", func(ctx context.Context, c *vgirpc.CallContext, p requestParams[GetObjectsRequest]) (ExecuteResponse, error) {
		return runSession(s, ctx, c, p.Request.SessionID, func(ss *session) (ExecuteResponse, error) {
			if e := validateRequest(p.Request); e != nil {
				return ExecuteResponse{}, e
			}
			q, e := ss.conn.GetObjects(ctx, p.Request)
			if e != nil {
				return ExecuteResponse{}, e
			}
			return s.addResult(ss, "", q)
		})
	})
	namedUnary(rpc, "get_statistics", func(ctx context.Context, c *vgirpc.CallContext, p requestParams[GetStatisticsRequest]) (ExecuteResponse, error) {
		return runSession(s, ctx, c, p.Request.SessionID, func(ss *session) (ExecuteResponse, error) {
			if e := validateRequest(p.Request); e != nil {
				return ExecuteResponse{}, e
			}
			q, e := ss.conn.GetStatistics(ctx, p.Request)
			if e != nil {
				return ExecuteResponse{}, e
			}
			return s.addResult(ss, "", q)
		})
	})
	vgirpc.Unary(rpc, "get_table_types", func(ctx context.Context, c *vgirpc.CallContext, p sessionParams) (ExecuteResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (ExecuteResponse, error) {
			q, e := ss.conn.GetTableTypes(ctx)
			if e != nil {
				return ExecuteResponse{}, e
			}
			return s.addResult(ss, "", q)
		})
	})
	vgirpc.Unary(rpc, "get_statistic_names", func(ctx context.Context, c *vgirpc.CallContext, p sessionParams) (ExecuteResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (ExecuteResponse, error) {
			q, e := ss.conn.GetStatisticNames(ctx)
			if e != nil {
				return ExecuteResponse{}, e
			}
			return s.addResult(ss, "", q)
		})
	})

	namedUnary(rpc, "get_table_schema", func(ctx context.Context, c *vgirpc.CallContext, p requestParams[GetTableSchemaRequest]) (SchemaResponse, error) {
		return runSession(s, ctx, c, p.Request.SessionID, func(ss *session) (SchemaResponse, error) {
			if e := validateRequest(p.Request); e != nil {
				return SchemaResponse{}, e
			}
			schema, e := ss.conn.GetTableSchema(ctx, p.Request)
			if e != nil {
				return SchemaResponse{}, e
			}
			return s.schemaResponse(schema)
		})
	})
	namedUnary(rpc, "set_connection_option", func(ctx context.Context, c *vgirpc.CallContext, p requestParams[SetConnectionOptionRequest]) (OkResponse, error) {
		return runSession(s, ctx, c, p.Request.SessionID, func(ss *session) (OkResponse, error) {
			r := p.Request
			if !validKey(r.Key) || r.Value.validate() != nil {
				return ok(failure("invalid_arguments", "Invalid option"))
			}
			if authoritative(ss.target, r.Key) {
				return ok(failure("unauthorized", "Server option is authoritative"))
			}
			if !ss.target.AllowedConnectionOptions[r.Key] {
				return ok(failure("unauthorized", "Caller option is not allowed"))
			}
			return ok(ss.conn.SetOption(ctx, r.Key, r.Value))
		})
	})
	vgirpc.Unary(rpc, "get_connection_option", func(ctx context.Context, c *vgirpc.CallContext, p connectionOptionParams) (ValueResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (ValueResponse, error) {
			if !validKey(p.Key) || !validKind(p.ValueType) {
				return ValueResponse{}, failure("invalid_arguments", "Invalid option")
			}
			v, e := ss.conn.GetOption(ctx, p.Key, p.ValueType)
			return optionResponse(v, p.ValueType, e)
		})
	})
	namedUnary(rpc, "set_statement_option", func(ctx context.Context, c *vgirpc.CallContext, p requestParams[SetStatementOptionRequest]) (OkResponse, error) {
		r := p.Request
		return runStatement(s, ctx, c, r.SessionID, r.StatementID, func(ss *session, st *statement) (OkResponse, error) {
			if !validKey(r.Key) || r.Value.validate() != nil {
				return ok(failure("invalid_arguments", "Invalid option"))
			}
			if authoritative(ss.target, r.Key) {
				return ok(failure("unauthorized", "Server option is authoritative"))
			}
			return ok(st.backend.SetOption(ctx, r.Key, r.Value))
		})
	})
	vgirpc.Unary(rpc, "get_statement_option", func(ctx context.Context, c *vgirpc.CallContext, p statementOptionParams) (ValueResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (ValueResponse, error) {
			if !validKey(p.Key) || !validKind(p.ValueType) {
				return ValueResponse{}, failure("invalid_arguments", "Invalid option")
			}
			v, e := st.backend.GetOption(ctx, p.Key, p.ValueType)
			return optionResponse(v, p.ValueType, e)
		})
	})
	vgirpc.Unary(rpc, "execute_partitions", func(ctx context.Context, c *vgirpc.CallContext, p statementParams) (PartitionsResponse, error) {
		return runStatement(s, ctx, c, p.SessionID, p.StatementID, func(ss *session, st *statement) (PartitionsResponse, error) {
			s.closeResult(ss, st.resultID)
			v, e := st.backend.ExecutePartitions(ctx)
			if e != nil {
				return PartitionsResponse{}, e
			}
			if v == nil || v.RowsAffected < -1 || len(v.Partitions) > s.limits.Partitions {
				return PartitionsResponse{}, failure("invalid_data", "Partition limit exceeded")
			}
			schema, e := s.schemaResponse(v.Schema)
			if e != nil {
				return PartitionsResponse{}, e
			}
			parts := make([][]byte, len(v.Partitions))
			size := len(schema.SchemaIPC)
			for i, p := range v.Partitions {
				parts[i], e = s.sealPartition(ss.owner, ss.targetName, p)
				if e != nil {
					return PartitionsResponse{}, e
				}
				size += len(parts[i])
				if size > s.limits.BatchBytes {
					return PartitionsResponse{}, failure("invalid_data", "Partition response limit exceeded")
				}
			}
			return PartitionsResponse{v.RowsAffected, schema.SchemaIPC, parts}, nil
		})
	})
	vgirpc.Unary(rpc, "read_partition", func(ctx context.Context, c *vgirpc.CallContext, p partitionParams) (ExecuteResponse, error) {
		return runSession(s, ctx, c, p.SessionID, func(ss *session) (ExecuteResponse, error) {
			raw, e := s.openPartition(ss.owner, ss.targetName, p.Payload)
			if e != nil {
				return ExecuteResponse{}, e
			}
			q, e := ss.conn.ReadPartition(ctx, raw)
			if e != nil {
				return ExecuteResponse{}, e
			}
			return s.addResult(ss, "", q)
		})
	})
	for _, stream := range []bool{false, true} {
		name := "bind"
		if stream {
			name = "bind_stream"
		}
		isStream := stream
		vgirpc.Exchange(rpc, name, okSchema, bindSchema, func(ctx context.Context, c *vgirpc.CallContext, p bindParams) (*vgirpc.StreamResult, error) {
			return s.startBind(ctx, c, p, isStream)
		})
	}
}
func validKind(s string) bool { return s == "string" || s == "bytes" || s == "int" || s == "double" }
func optionResponse(v OptionValue, kind string, e error) (ValueResponse, error) {
	if e != nil {
		return ValueResponse{}, e
	}
	if v.validate() != nil || v.Kind != kind {
		return ValueResponse{}, failure("invalid_data", "Backend returned invalid option")
	}
	return ValueResponse{v}, nil
}
func (s *Service) schemaResponse(schema *arrow.Schema) (SchemaResponse, error) {
	b, e := schemaIPC(schema)
	if e != nil {
		return SchemaResponse{}, e
	}
	if len(b) > s.limits.BatchBytes {
		return SchemaResponse{}, failure("invalid_data", "Schema limit exceeded")
	}
	return SchemaResponse{b}, nil
}
func validateRequest(r any) error {
	bad := false
	check := func(v *string) {
		if v != nil && !validText(*v) {
			bad = true
		}
	}
	switch p := r.(type) {
	case GetInfoRequest:
		if p.Codes != nil {
			for _, c := range *p.Codes {
				if c < 0 || c >= 1<<32 {
					bad = true
				}
			}
		}
	case GetObjectsRequest:
		bad = p.Depth < 0 || p.Depth > 3
		check(p.Catalog)
		check(p.DBSchema)
		check(p.TableName)
		check(p.ColumnName)
		if p.TableTypes != nil {
			for _, v := range *p.TableTypes {
				if !validText(v) {
					bad = true
				}
			}
		}
	case GetStatisticsRequest:
		check(p.Catalog)
		check(p.DBSchema)
		check(p.TableName)
	case GetTableSchemaRequest:
		check(p.Catalog)
		check(p.DBSchema)
		bad = bad || !validText(p.TableName)
	}
	if bad {
		return failure("invalid_arguments", "Invalid metadata request")
	}
	return nil
}
