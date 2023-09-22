/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package server

import (
	"context"
	"github.com/sprintframework/raftapi"
	"github.com/sprintframework/raftpb"
	"github.com/recordbase/recordbasepb"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"time"
)


func (t *implAPIServer) updateMethod(ctx context.Context,  cmd *recordbasepb.Command) error {

	return t.doWithRaft(ctx, func(ctx context.Context, r *raft.Raft) error {
		_, err := t.applyCommand(ctx, r, cmd)
		return err
	})

}

func (t *implAPIServer) doWithRaft(ctx context.Context, cb func(ctx context.Context, r *raft.Raft) error) (err error) {

	r, ok := t.RaftServer.Raft()
	if !ok {
		return ErrRaftNotInitialized
	}

	return cb(ctx, r)
}

func (t *implAPIServer) doAuthorized(ctx context.Context, methodName string, cb func(ctx context.Context) error) (err error) {

	user, ok := t.AuthorizationMiddleware.GetUser(ctx)
	if !ok || !user.Roles["USER"] {
		return status.Errorf(codes.Unauthenticated, "role USER is required")
	}

	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				err = v
			case string:
				err = errors.New(v)
			default:
				err = errors.Errorf("%v", v)
			}
		}

		if err != nil {
			err = t.wrapError(err, methodName, user.Username)
		}
	}()

	return cb(ctx)
}


func (t *implAPIServer) wrapError(err error, method, username string) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	issue := err.Error()
	if strings.HasPrefix(issue, "nowrap:") {
		issue = strings.TrimSpace(strings.TrimPrefix(issue, "nowrap:"))
		return errors.New(issue)
	}
	message := "internal error"
	if strings.Contains("concurrent transaction", issue) {
		message = "concurrent transaction"
	} else if strings.Contains("not found", issue) {
		message = "object not found"
	} else if strings.Contains("exist", issue) {
		message = "object already exist"
	}
	id := t.NodeService.Issue().String()
	t.Log.Error(method, zap.String("errorId", id), zap.Any("username", username), zap.Error(err))
	return status.Errorf(codes.Internal, "%s %s", message, id)
}

func (t *implAPIServer) applyCommand(ctx context.Context, r *raft.Raft, cmd *recordbasepb.Command) (*raftpb.Status, error) {

	payload, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	raftCmd := &raftpb.Command{Payload: payload}

	if r.State() != raft.Leader {
		leaderAddress := r.Leader()
		if string(leaderAddress) == "" {
			return nil, ErrRaftLeaderNotFound
		}
		leaderConn, err := t.RaftClientPool.GetAPIConn(leaderAddress)
		if err != nil {
			return nil, err
		}
		leaderClient := raftpb.NewRaftServiceClient(leaderConn)
		return leaderClient.ApplyCommand(ctx, raftCmd)
	}

	start := time.Now()
	f := r.Apply(payload, t.RaftTimeout)
	err = f.Error()

	if err != nil {
		return nil, err
	}

	resp := f.Response()
	if r, ok := resp.(raftapi.FSMResponse); ok {
		r.Status.Elapsed = time.Since(start).Seconds()
		return r.Status, r.Err
	}

	return nil, errors.Errorf("invalid raft response %v", resp)

}

