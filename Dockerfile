FROM golang:1.12-stretch as builder

WORKDIR /fake-gcs

COPY . /fake-gcs

ENV CGO_ENABLED=0
ENV GOOS=linux

RUN go build -o /fake-gcs/server

FROM alpine:3.9

COPY --from=builder /fake-gcs/server /fake-gcs

EXPOSE 4443

ENTRYPOINT [ "/fake-gcs" ]
