/*
 * Copyright (c) 2022-2023 Zander Schwid & Co. LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package server

import (
	"context"
	"crypto/tls"
	"github.com/codeallergy/glue"
	"github.com/openraft/raftapi"
	"github.com/sprintframework/sprint"
	"github.com/sprintframework/sprintframework/pkg/util"
	"github.com/keyvalstore/store"
	"github.com/recordbase/recordpb"
	"github.com/recordbase/recordbaseserv/pkg/api"
	"github.com/recordbase/recordbaseserv/pkg/pb"
	"github.com/pkg/errors"
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
	recordpb.UnimplementedRecordServiceServer
	pb.UnimplementedAdminServiceServer

	GrpcServer       *grpc.Server   `inject`
	APIGatewayServer  *http.Server   `inject:"bean=api-gateway-server"`

	Tls            bool     `value:"api-grpc-server.tls,default=false"`
	GrpcAddress    string   `value:"api-grpc-server.listen-address,default="`

	Properties              glue.Properties                `inject`
	AuthorizationMiddleware sprint.AuthorizationMiddleware `inject`
	NodeService             sprint.NodeService             `inject`
	RecordService           api.RecordService              `inject`

	RaftServer            raftapi.RaftServer      `inject`
	RaftClientPool        raftapi.RaftClientPool  `inject`

	TransactionalManager  store.TransactionalManager  `inject:"bean=record-storage"`

	RaftTimeout      time.Duration   `value:"raft.timeout,default=10s"`

	Log             *zap.Logger          `inject`

}

func APIServer() api.GRPCServer {
	return &implAPIServer{}
}

func (t *implAPIServer) PostConstruct() error {
	recordpb.RegisterRecordServiceServer(t.GrpcServer, t)
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
		return errors.New("property 'api-grpc-server.listen-address' is empty")
	}

	recordpb.RegisterRecordServiceHandlerFromEndpoint(context.Background(), api, t.GrpcAddress, opts)

	return nil
}

func (t *implAPIServer) BeanName() string {
	return "grpc_api_server"
}

func (t *implAPIServer) GetStats(cb func(name, value string) bool) error {
	return nil
}

