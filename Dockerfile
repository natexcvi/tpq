FROM golang:1.18 AS build
WORKDIR /root/build/
COPY . ./
RUN go mod tidy
RUN GOOS=linux go build -a -o tpq .

FROM debian:bookworm-slim AS runtime
WORKDIR /root/server/
COPY --from=build /root/build/tpq ./
CMD ["./tpq"]