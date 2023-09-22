module github.com/recordbase/recordbaseserv

go 1.17

replace github.com/sprintframework/raftmod => ../../sprintframework/raftmod

replace github.com/hashicorp/serf => ../../hashicorp/serf

replace github.com/sprintframework/sprintframework => ../../sprintframework/sprintframework

require (
	github.com/codeallergy/glue v1.1.0
	github.com/codeallergy/go-bindata v1.0.0
	github.com/go-errors/errors v1.4.2
	github.com/golang/protobuf v1.5.2
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.15.2
	github.com/hashicorp/raft v1.5.0
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/keyvalstore/store v1.3.0
	github.com/pkg/errors v0.9.1
	github.com/recordbase/recordbasepb v1.3.0
	github.com/sprintframework/certmod v1.0.0
	github.com/sprintframework/dnsmod v1.0.0
	github.com/sprintframework/natmod v1.0.1
	github.com/sprintframework/raftapi v1.2.10
	github.com/sprintframework/raftmod v1.2.11-0.20230922215107-a2feda46b79e
	github.com/sprintframework/raftpb v1.2.10
	github.com/sprintframework/sealmod v1.0.0
	github.com/sprintframework/sprint v1.3.7
	github.com/sprintframework/sprintframework v1.3.7
	go.uber.org/zap v1.24.0
	google.golang.org/genproto v0.0.0-20230303212802-e74f57abe488
	google.golang.org/grpc v1.53.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.3.0
	google.golang.org/protobuf v1.28.1
)

require go.uber.org/atomic v1.10.0 // indirect

require github.com/openraft/raftgrpc v1.2.2

require (
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.1.1 // indirect
	github.com/Masterminds/sprig/v3 v3.2.1 // indirect
	github.com/armon/circbuf v0.0.0-20150827004946-bbbad097214e // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/bgentry/speakeasy v0.1.0 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/codeallergy/base62 v1.1.0 // indirect
	github.com/codeallergy/properties v1.1.0 // indirect
	github.com/codeallergy/uuid v1.1.0 // indirect
	github.com/dgraph-io/badger/v3 v3.2103.5 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-acme/lego/v4 v4.8.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/flatbuffers v23.1.21+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-hclog v1.5.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/go-syslog v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/hashicorp/logutils v1.0.0 // indirect
	github.com/hashicorp/mdns v1.0.4 // indirect
	github.com/hashicorp/memberlist v0.5.0 // indirect
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/huin/goupnp v1.2.0 // indirect
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/jackpal/go-nat-pmp v1.0.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/keyvalstore/badgerstore v1.3.0 // indirect
	github.com/keyvalstore/bboltstore v1.3.0 // indirect
	github.com/keyvalstore/boltstore v1.3.0 // indirect
	github.com/keyvalstore/cachestore v1.3.0 // indirect
	github.com/klauspost/compress v1.16.0 // indirect
	github.com/likexian/whois v1.14.2 // indirect
	github.com/mailgun/mailgun-go/v4 v4.8.1 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/miekg/dns v1.1.50 // indirect
	github.com/mitchellh/cli v1.1.5 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/openraft/raftapi v1.2.5 // indirect
	github.com/openraft/raftpb v1.2.0 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/posener/complete v1.2.3 // indirect
	github.com/ryanuber/columnize v2.1.2+incompatible // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/sprintframework/cert v1.0.0 // indirect
	github.com/sprintframework/certpb v1.0.0 // indirect
	github.com/sprintframework/dns v1.0.0 // indirect
	github.com/sprintframework/nat v1.0.0 // indirect
	github.com/sprintframework/raft-badger v1.2.2 // indirect
	github.com/sprintframework/seal v1.0.0 // indirect
	github.com/sprintframework/sprintpb v1.3.0 // indirect
	go.etcd.io/bbolt v1.3.7 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/crypto v0.10.0 // indirect
	golang.org/x/mod v0.8.0 // indirect
	golang.org/x/net v0.11.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/sys v0.9.0 // indirect
	golang.org/x/term v0.9.0 // indirect
	golang.org/x/text v0.10.0 // indirect
	golang.org/x/tools v0.6.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/square/go-jose.v2 v2.6.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	software.sslmate.com/src/go-pkcs12 v0.2.0 // indirect
)
