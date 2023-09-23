/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package server

import (
	"context"
	"crypto/tls"
	"github.com/codeallergy/glue"
	"github.com/keyvalstore/store"
	"github.com/sprintframework/raftapi"
	"github.com/pkg/errors"
	"github.com/recordbase/recordbaseserv/pkg/api"
	"github.com/recordbase/recordbaseserv/pkg/pb"
	"github.com/recordbase/recordbasepb"
	"github.com/sprintframework/sprint"
	"github.com/sprintframework/sprintframework/pkg/util"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"net/http"
	"time"
)

var empty = &emptypb.Empty{}

var (
	ErrRaftNotInitialized = status.Error(codes.Unavailable, "raft not initialized")
	ErrRaftLeaderNotFound = status.Error(codes.Unavailable, "raft leader not found")

	ErrNotFound = status.Error(codes.NotFound, "entity not found")
)

type implAPIServer struct {
	recordbasepb.UnimplementedRecordServiceServer
	pb.UnimplementedAdminServiceServer

	GrpcServer       *grpc.Server   `inject`
	APIGatewayServer  *http.Server   `inject:"bean=api-gateway-server"`

	Tls            bool     `value:"api-grpc-server.tls,default=false"`
	GrpcAddress    string   `value:"api-grpc-server.bind-address,default="`

	Properties              glue.Properties                `inject`
	AuthorizationMiddleware sprint.AuthorizationMiddleware `inject`
	NodeService             sprint.NodeService             `inject`
	RecordService           api.RecordService              `inject`

	RaftServer            raftapi.RaftServer      `inject`
	RaftClientPool        raftapi.RaftClientPool  `inject`

	TransactionalManager  store.TransactionalManager  `inject:"bean=record-store"`

	RaftTimeout      time.Duration   `value:"raft.timeout,default=10s"`

	Log             *zap.Logger          `inject`

}

func APIServer() api.GRPCServer {
	return &implAPIServer{}
}

func (t *implAPIServer) PostConstruct() error {
	recordbasepb.RegisterRecordServiceServer(t.GrpcServer, t)
	pb.RegisterAdminServiceServer(t.GrpcServer, t) // no gateway

	api, err := util.FindGatewayHandler(t.APIGatewayServer, "/api/")
	if err != nil {
		return err
	}

	// interceptors do not work yet
	//pb.RegisterAuthServiceHandlerServer(context.Background(), api, t)

	var opts []grpc.DialOption

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos: []string {"h2"},
	}

	tlsCredentials := credentials.NewTLS(tlsConfig)
	opts = append(opts, grpc.WithTransportCredentials(tlsCredentials))

	if t.GrpcAddress == "" {
		return errors.New("property 'api-grpc-server.bind-address' is empty")
	}

	recordbasepb.RegisterRecordServiceHandlerFromEndpoint(context.Background(), api, t.GrpcAddress, opts)
	/*
	if ts, ok := t.RaftServer.Transport(); ok {
		addr := ts.LocalAddr()
		fmt.Printf("Transport Addr: %v\n", addr)
		if raft, ok := t.RaftServer.Raft(); ok {
			nodeId := t.NodeService.NodeIdHex()
			for _, server := range raft.GetConfiguration().Configuration().Servers {
				if string(server.ID) == nodeId {
					fmt.Printf("Raft Addr: %v\n", server.Address)
					if server.Address != addr {
						raft.Stats()
					}
				}
			}

		}
	}
	*/
	return nil
}

func (t *implAPIServer) BeanName() string {
	return "grpc_api_server"
}

func (t *implAPIServer) GetStats(cb func(name, value string) bool) error {
	return nil
}

