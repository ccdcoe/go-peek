# build stage
FROM golang AS Builder

RUN apt-get update && apt-get install -y gcc git
RUN mkdir -p /go/src/github.com/ccdcoe/

COPY . /go/src/github.com/ccdcoe/go-peek/
WORKDIR /go/src/github.com/ccdcoe/go-peek
ENV GO111MODULE "on"
ENV CGO_ENABLED 0

RUN go build -o /tmp/peek .

# final stage
FROM alpine
COPY --from=Builder /tmp/peek /usr/local/bin/

ENV user=peek
ENV gid=61000
ENV working_dir=/var/lib/peek

RUN addgroup -S $gid && adduser --disabled-password -G $gid -H -s /bin/false -u $gid -h $working_dir $user
RUN mkdir -p $working_dir && chown -R $user:$gid $working_dir
VOLUME $working_dir
USER $user
WORKDIR $working_dir

ENTRYPOINT ["/usr/local/bin/peek"]
