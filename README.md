# recordbaseserv

Recordbase Server is the simple key-value server that uses Raft protocol for communication between nodes.

![build workflow](https://github.com/recordbase/recordbaseserv/actions/workflows/build.yaml/badge.svg)

# Build

Local build on golang 1.17

Install golang 1.17.13
```
wget [go1.17.13.darwin-amd64.pkg](https://go.dev/dl/go1.17.13.darwin-amd64.pkg)
```

Fix Goland IDE
```
sudo nano /usr/local/go/src/runtime/internal/sys/zversion.go
```
Add line:
```
const TheVersion = `go1.17`
```

Setup local environments 
```
cd ~
nano .zshrc
```

Add lines (for MacOS)
```
export GOROOT=/usr/local/go/
export GOPATH=/Users/$USER/go
export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
```

Download and install Protoc 3.20.3 for your OS (optional)
```
open https://github.com/protocolbuffers/protobuf/releases/tag/v3.20.3
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.20.3/protoc-3.20.3-osx-x86_64.zip
unzip protoc-3.20.3-osx-x86_64.zip -d ~/go
```

Verify
```
protoc --version
libprotoc 3.20.3
```

```
make deps
make
```

Build in docker

```
make docker
```

# Run

Generate bootstrap token

```
./recordbase token boot
```

Store it in environment RECORDBASE_BOOT or you would need to enter it manually every time.

Restart terminal to reflect new variable.

Run first time
```
 ./recordbase run
```

Find in console output `export RECORDBASE_AUTH=....`

Store it in environment variables or you would need to include this JWT token in each console command.

Restart terminal to reflect variables.

Start service
```
./recordbase start
```

Check status
```
./recordbase status
```

Stop service
```
./recordbase stop
```

