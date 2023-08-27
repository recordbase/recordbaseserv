# Copyright (c) Zander Schwid & Co. LLC.
# SPDX-License-Identifier: BUSL-1.1

FROM codeallergy/ubuntu-golang as builder

ARG VERSION
ARG BUILD

WORKDIR /go/src/github.com/recordbase/recordbaseserv
ADD . .

ENV GONOSUMDB github.com

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive \
    apt-get install --no-install-recommends --assume-yes \
    autoconf automake libtool curl make g++ unzip

RUN bash .github/scripts/install-protoc.sh 3.20.3
RUN make deps
RUN make

FROM ubuntu:18.04
WORKDIR /app/bin

COPY --from=builder /go/src/github.com/recordbase/recordbaseserv/recordbase .

EXPOSE 8080 8443 8444

CMD ["/app/bin/recordbase", "run"]

