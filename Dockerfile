FROM golang:1.19-alpine3.16 as Builder

RUN apk update
RUN apk --no-cache --update add build-base
RUN apk --no-cache --update add git
RUN apk --no-cache --update add ca-certificates
RUN apk --no-cache --update add tzdata
RUN apk --no-cache --update add gcc
RUN apk --no-cache --update add g++
RUN apk --no-cache --update add librdkafka-dev
RUN apk --no-cache --update add musl-dev

WORKDIR /kafka-consumer
COPY . .

RUN go test -v  ./... -tags musl
RUN go install -tags musl

FROM alpine as final

ENV TZ Asia/Bangkok

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /go/bin/kafka-consumer /go/bin/kafka-consumer

CMD ["/go/bin/kafka-consumer"]