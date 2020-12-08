FROM golang:latest as builder

ENV GO111MODULE on
ENV GOPROXY=https://goproxy.cn,direct

# WORKDIR /go/cache

# COPY go.mod .
# COPY go.sum .
# RUN go mod download

WORKDIR /go/release

COPY . .

RUN export GOPROXY="https://goproxy.cn,direct" \
    && go install github.com/prometheus/promu \
    && promu build 

FROM scratch as prod

# use the time.LoadLocation could find the zoneinfo, if not set, while return error
COPY --from=builder /usr/local/go/lib/time/zoneinfo.zip /zoneinfo.zip
COPY --from=builder /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY --from=builder /go/release/prom2click /

ENV ZONEINFO /zoneinfo.zip

EXPOSE 9201

ENTRYPOINT [ "/prom2click" ]