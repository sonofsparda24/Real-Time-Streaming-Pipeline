FROM alpine:edge

RUN apk add --no-cache \
    syslog-ng \
    syslog-ng-json \
    syslog-ng-scl \
    syslog-ng-kafka \
    syslog-ng-http

ENTRYPOINT ["syslog-ng", "-F", "--no-caps"]
