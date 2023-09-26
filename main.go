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
	"github.com/sprintframework/certmod"
	"github.com/sprintframework/dnsmod"
	"github.com/sprintframework/natmod"
	"github.com/sprintframework/raftmod"
	"github.com/sprintframework/raftmod/raftcmd"
	"github.com/sprintframework/sealmod"
	"github.com/sprintframework/sprint"
	"github.com/sprintframework/sprintframework/sprintapp"
	"github.com/sprintframework/sprintframework/sprintclient"
	"github.com/sprintframework/sprintframework/sprintcmd"
	"github.com/sprintframework/sprintframework/sprintcore"
	"github.com/sprintframework/sprintframework/sprintserver"
	"github.com/sprintframework/sprintframework/sprintutils"
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

	sprintutils.PanicToError(&err)

	beans := []interface{} {
		sprintapp.ApplicationScanner(
			AppResources,
			sprintcmd.DefaultCommands,
			/*raftgrpc.RaftCommand(), */
			raftcmd.Scan),

		glue.Child(sprint.CoreRole,
			sprintcore.CoreScanner(),
			natmod.Scanner(),
			dnsmod.Scanner(),
			sealmod.Scanner(),
			certmod.Scanner(),
			sprintcore.BadgerStoreFactory("config-store"),
			sprintcore.BadgerStoreFactory("record-store"),
			sprintcore.BadgerStoreFactory("raft-store"),
			sprintcore.LumberjackFactory(),
			sprintcore.AutoupdateService(),
			service.Scan,

			glue.Child(sprint.ServerRole,
				sprintserver.GrpcServerScanner("control-grpc-server"),
				sprintserver.ControlServer(),
				sprintserver.TlsConfigFactory("tls-config"),
			),

			glue.Child(sprint.ServerRole,
				sprintserver.GrpcServerScanner("api-grpc-server"),
				server.APIServer(),
				raftmod.Scan,
				//raftgrpc.RaftGrpcServer(),
				sprintserver.HttpServerFactory("api-gateway-server"),
				sprintserver.TlsConfigFactory("tls-config"),
			),

		),
		glue.Child(sprint.ControlClientRole,
			sprintclient.ControlClientScanner(),
			sprintclient.AnyTlsConfigFactory("tls-config"),
		),
		glue.Child("api",
			sprintclient.GrpcClientFactory("api-grpc-client"),
			sprintclient.AnyTlsConfigFactory("tls-config"),
		),
	}

	return sprintapp.Application("recordbase",
		sprintapp.WithVersion(Version),
		sprintapp.WithBuild(Build),
		sprintapp.WithBeans(beans)).
	    Run(os.Args[1:])

}

func main() {

	if err := doMain(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	time.Sleep(100 * time.Millisecond)
}
