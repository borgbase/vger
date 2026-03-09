FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

ARG TARGETPLATFORM
COPY bin/${TARGETPLATFORM}/vykar /usr/local/bin/vykar
COPY bin/${TARGETPLATFORM}/vykar-server /usr/local/bin/vykar-server

RUN mkdir -p /etc/vykar /data /repo /cache

ENV XDG_CACHE_HOME=/cache

WORKDIR /data

ENTRYPOINT ["vykar"]
CMD ["daemon"]
