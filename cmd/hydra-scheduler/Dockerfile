# Build context: repository root. One image per store:
#   docker buildx build -f cmd/hydra-scheduler/Dockerfile \
#     --build-arg STORE=redis --platform linux/amd64,linux/arm64 \
#     -t hydra-scheduler:redis .
# STORE is sqlite (default), redis (also Valkey and Dragonfly)
# or postgres. See the Makefile targets (make help).

# xx cross-compiles for the target platform from the build
# platform, so arm64 builds natively fast on amd64 (and vice
# versa) instead of under QEMU emulation.
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.6.1 AS xx

FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine AS build
COPY --from=xx / /
RUN apk add --no-cache clang lld ca-certificates

ARG TARGETPLATFORM
ARG STORE=sqlite
# Only SQLite needs CGO (and a C toolchain for the target);
# musl lets it be linked fully static. The other stores are
# pure Go.
RUN if [ "$STORE" = sqlite ]; then xx-apk add --no-cache musl-dev gcc; fi

WORKDIR /src
COPY . .

ARG VERSION=dev

# timetzdata embeds the zone database (scratch has none and
# recurrence_timezone needs it); netgo and osusergo keep DNS and
# user lookups in Go instead of libc. The STORE tag compiles in
# that store only.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -e; \
    tags="timetzdata netgo osusergo $STORE"; \
    ldflags="-s -w -X main.version=${VERSION}"; \
    case "$STORE" in \
      sqlite) export CGO_ENABLED=1; tags="$tags sqlite_omit_load_extension"; \
              ldflags="$ldflags -linkmode external -extldflags -static" ;; \
      redis|postgres) export CGO_ENABLED=0 ;; \
      *) echo "unknown STORE '$STORE' (sqlite, redis, postgres)"; exit 1 ;; \
    esac; \
    xx-go build -trimpath -tags "$tags" -ldflags "$ldflags" \
      -o /out/hydra-scheduler-$STORE ./cmd/hydra-scheduler; \
    xx-verify --static /out/hydra-scheduler-$STORE; \
    ln /out/hydra-scheduler-$STORE /out/hydra-scheduler

# Working directory for the SQLite store (the default
# HYDRA_STORE sqlite:hydra.db lands in /data), owned by the
# unprivileged runtime user.
RUN mkdir -p /out/data && chown 65532:65532 /out/data

# Only the binary, named after its store, for `docker buildx
# build --target binary --output type=local,dest=...` (make
# scheduler-binaries).
FROM scratch AS binary
ARG STORE=sqlite
COPY --from=build /out/hydra-scheduler-${STORE} /

FROM scratch

ARG STORE=sqlite
ARG VERSION=dev

LABEL org.opencontainers.image.title="hydra-scheduler (${STORE})" \
      org.opencontainers.image.description="Recurring event scheduler for NATS JetStream, ${STORE} store" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="https://github.com/zsweiter/hydra" \
      org.opencontainers.image.licenses="MIT"

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/hydra-scheduler /hydra-scheduler
COPY --from=build --chown=65532:65532 /out/data /data

WORKDIR /data

USER 65532:65532

ENTRYPOINT ["/hydra-scheduler"]
