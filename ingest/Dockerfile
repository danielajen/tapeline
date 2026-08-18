# Static Go binary on a distroless base.
#
# The result is roughly 20MB and contains no shell, no package manager and no
# libc. That is not minimalism for its own sake: a container with no shell is
# a container an attacker cannot get a shell in, and CVE scanners have almost
# nothing to report because there is almost nothing there.

FROM golang:1.24-alpine AS build

WORKDIR /src

# Dependencies first, as their own layer. Source edits then rebuild in
# seconds rather than re-downloading the module cache every time.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off produces a genuinely static binary, which is what allows the
# distroless static base rather than one carrying glibc.
# -trimpath keeps build-host paths out of the binary; the ldflags strip debug
# information and stamp the version.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/ingestd ./cmd/ingestd

# Run the tests inside the image build so a container can never be produced
# from code that does not pass them.
RUN go vet ./... && go test ./...

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ingestd /usr/local/bin/ingestd

# Non-root by default, and the image declares it so a Kubernetes
# runAsNonRoot check has something to verify.
USER nonroot:nonroot

EXPOSE 9101

ENTRYPOINT ["/usr/local/bin/ingestd"]
