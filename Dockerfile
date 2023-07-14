FROM golang:1.19-alpine
RUN apk add build-base
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN go build -tags netgo -ldflags '-s -w' -o edgeurid

CMD [ "./edgeurid daemon" ]
EXPOSE 1313
