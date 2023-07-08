#!/usr/bin/env bash
VERSION=$1
DOWNLOAD_LINK=https://github.com/protocolbuffers/protobuf/releases/download/v$VERSION/protoc-$VERSION-linux-x86_64.zip
curl -LO $DOWNLOAD_LINK
unzip protoc-$VERSION-linux-x86_64.zip -d /usr/local

