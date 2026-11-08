# One Dockerfile for all binaries; pick one with --build-arg CMD=api|worker|outbox-relay|operator-mock.
# Same idea as a .NET multi-stage build (sdk image -> aspnet runtime image).

# ---- build stage: full Go toolchain ----
FROM golang:1.22-alpine AS build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src

# Download modules first, so this layer stays cached until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG CMD
RUN test -n "$CMD" || (echo "build arg CMD is required" && exit 1)
# -trimpath: no local paths in the binary; -s -w: strip debug symbols (smaller image).
RUN go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${CMD}

# ---- runtime stage: a static binary on a small base ----
# alpine instead of distroless/scratch: it has busybox wget for healthchecks and
# a shell for debugging, for about 7 MB.
FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/service /app/service
USER app
ENTRYPOINT ["/app/service"]
