# The hub only. Agents run natively on each machine; they are not containers.
FROM rust:1-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p fleetmon-hub

# distroless/cc: glibc and nothing else — no shell, no package manager. The hub
# is one static-ish binary that needs only libc.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/fleetmon-hub /usr/local/bin/fleetmon-hub
ENV FLEETMON_BIND=0.0.0.0:7070
EXPOSE 7070
ENTRYPOINT ["/usr/local/bin/fleetmon-hub"]
