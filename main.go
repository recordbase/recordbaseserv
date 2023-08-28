/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package main

import (
	"fmt"
	"github.com/codeallergy/glue"
	"github.com/recordbase/recordbaseserv/pkg/resources"
	"github.com/recordbase/recordbaseserv/pkg/server"
	"github.com/recordbase/recordbaseserv/pkg/service"
	"github.com/openraft/raftmod"
	"github.com/openraft/raftgrpc"
	"github.com/pkg/errors"
	"github.com/sprintframework/certmod"
	"github.com/sprintframework/dnsmod"
	"github.com/sprintframework/natmod"
	"github.com/sprintframework/sealmod"
	"github.com/sprintframework/sprintframework/pkg/app"
	sprintclient "github.com/sprintframework/sprintframework/pkg/client"
	sprintcmd "github.com/sprintframework/sprintframework/pkg/cmd"
	sprintcore "github.com/sprintframework/sprintframework/pkg/core"
	sprintserver "github.com/sprintframework/sprintframework/pkg/server"
	"os"
	"time"
)

var (
	Version string
	Build   string
)

var AppResources = &glue.ResourceSource{
	Name: "resources",
	AssetNames: resources.AssetNames(),
	AssetFiles: resources.AssetFile(),
}

func doMain() (err error) {

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
	}()

	return app.Application("recordbase",
		app.WithVersion(Version),
		app.WithBuild(Build),
		app.Beans(app.DefaultApplicationBeans, AppResources, sprintcmd.DefaultCommands, raftgrpc.RaftCommand()),
		app.Core(sprintcore.CoreScanner(
			natmod.Scanner(),
			dnsmod.Scanner(),
			sealmod.Scanner(),
			certmod.Scanner(),
			sprintcore.BadgerStorageFactory("config-storage"),
			sprintcore.BadgerStorageFactory("record-storage"),
			sprintcore.BadgerStorageFactory("raft-storage"),
			sprintcore.LumberjackFactory(),
			sprintcore.AutoupdateService(),
			service.Scan,
		)),
		app.Server(sprintserver.ServerScanner(
			sprintserver.AuthorizationMiddleware(),
			sprintserver.GrpcServerFactory("control-grpc-server"),
			sprintserver.ControlServer(),
			sprintserver.TlsConfigFactory("tls-config"),
		)),
		app.Client(sprintclient.ControlClientScanner(
			sprintclient.AnyTlsConfigFactory("tls-config"),
		)),
		app.Server(sprintserver.ServerScanner(
			sprintserver.AuthorizationMiddleware(),
			sprintserver.GrpcServerFactory("api-grpc-server"),
			server.APIServer(),
			raftmod.Scan,
			raftgrpc.RaftGrpcServer(),
			sprintserver.HttpServerFactory("api-gateway-server"),
			sprintserver.TlsConfigFactory("tls-config"),
		)),
		app.Client(sprintclient.ClientScanner("api",
			sprintclient.GrpcClientFactory("api-grpc-client"),
			sprintclient.AnyTlsConfigFactory("tls-config"),
		)),
	).Run(os.Args[1:])

}

func main() {

	if err := doMain(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	time.Sleep(100 * time.Millisecond)
}
