FROM codeallergy/ubuntu-golang as builder

ARG VERSION
ARG BUILD

WORKDIR /go/src/github.com/recordbase/recordbaseserv
ADD . .

ENV GONOSUMDB github.com

RUN go build -o /recordbase -v -ldflags "-X main.Version=$(VERSION) -X main.Build=$(BUILD)"

FROM ubuntu:18.04
WORKDIR /app/bin

COPY --from=builder /recordbase .

EXPOSE 8080 8443 8444

CMD ["/app/bin/recordbase", "run"]

