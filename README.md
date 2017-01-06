# logspout-firehose

[Logspout](https://github.com/gliderlabs/logspout) adapter for writing Docker container logs to [AWS Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/)

## Deploy

Build a logspout image with this firehose adapter (see Build section below).

Then run using whatever docker launch method you prefer. The important things to note are the AWS config ENV vars, the volume mount for docker socket, and the firehose launch command.

```
$ docker run --name="logspout" \
    --volume=/var/run/docker.sock:/var/run/docker.sock \
    -e AWS_ACCESS_KEY_ID=... \
    -e AWS_REGION=us-east-1 \
    -e AWS_SECRET_ACCESS_KEY=... \
    your-logspout-image \
    firehose://delivery-stream-name
```

See the logspout docs for more launch options.

## Build

To use, you must build a container image of logspout which contains the firehose adapter.

The easiest way to do this reliably is to fork the official logspout repo and add this this repo to the `modules.go` file.

Then `docker build . -t <your-image-tag>` in your forked logspout repo.

I find this approach to work best as it ensures you use a known version of logspout as the base.

Side note: GO dependencies and imports are idiotic constructs so if your fork of logspout breaks later because it is trying to download incompatible versions of dependencies via `go get` just blame the stupidity of GO.

## Development

Similar to the above Build process except you should use the `Dockerfile.dev` in the logspout repo (if it still exists) to build your base logspout image.

In your logspout repo clone/fork.

```
docker build -t logspout -f Dockerfile.dev .`
```

Then use the `dev.sh` script in this repo to launch a container with a shell where you will build and run logspout as you iterate.

In the container, run the following as you make changes.

```
go build -ldflags "-X main.Version=dev" -o /bin/logspout
/bin/logspout firehose://stream-name
```
