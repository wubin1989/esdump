FROM golang:1.16.6-alpine AS builder

ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /repo

ADD . .

RUN go mod tidy && go build

FROM alpine

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add --no-cache bash tzdata

WORKDIR /repo

COPY --from=builder /repo/esdump ./

ENTRYPOINT ["/repo/esdump"]
