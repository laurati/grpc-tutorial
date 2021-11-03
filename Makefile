compile:
	@protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/service.proto 

server:
	@go run server/main.go


client:
	@go run client/main.go