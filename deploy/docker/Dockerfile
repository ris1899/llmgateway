# one Dockerfile, two binaries: build with --build-arg CMD=gateway or mockprovider
# distroless static final image: no shell, no package manager, tiny attack surface
FROM golang:1.26-alpine AS build
WORKDIR /src
# cache the download layer separately from the code layer
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD=gateway
# CGO_ENABLED=0 for a static binary; -trimpath -s -w for reproducible, smaller output
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
