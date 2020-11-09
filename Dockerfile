FROM golang:alpine as build
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o moresql cmds/moresql/main.go

FROM alpine
WORKDIR /app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/moresql .
COPY --from=build /app/bin ./bin
CMD ["/app/moresql"]
