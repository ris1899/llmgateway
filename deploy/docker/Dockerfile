# One Dockerfile, two binaries: build with --build-arg CMD=gateway or mockprovider.
#
# Multi-stage: the Go toolchain (~800 MB) stays in the build stage; the final
# image is distroless static (~2 MB base) with no shell or package manager,
# which shrinks both image size and attack surface.
FROM golang:1.26-alpine AS build
WORKDIR /src
# Copy go.mod/go.sum first so the dependency download layer is cached and only
# re-runs when dependencies change, not on every code edit.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD=gateway
# CGO_ENABLED=0: fully static binary, runs on distroless/static without libc.
# -trimpath and -s -w: reproducible paths, smaller binary (no debug symbols).
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
