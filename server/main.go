package main

import (
	"context"
	proto "grpc-tutorial/proto"
	"net"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	proto.UnimplementedAddServiceServer
	//conn *pgx.Conn
}

func (s *server) Add(ctx context.Context, request *proto.Request) (*proto.Response, error) {
	a, b := request.GetA(), request.GetB()

	result := a + b

	return &proto.Response{Result: result}, nil
}

func (s *server) Multiply(ctx context.Context, request *proto.Request) (*proto.Response, error) {
	a, b := request.GetA(), request.GetB()

	result := a * b

	return &proto.Response{Result: result}, nil
}

func (s *server) GetData(ctx context.Context, request *proto.GeDataParams) (*proto.DataList, error) {

	var data_list *proto.DataList = &proto.DataList{}

	c := context.Background()

	projectID := "testezeus-328313"
	rows, err := bigquery.NewClient(c, projectID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	q := rows.Query(
		"SELECT datatype_id, data_min, data_max FROM `testezeus-328313.athena.Pixel` LIMIT 10")

	q.Location = "US"

	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}
	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	for {
		var row []bigquery.Value

		err := it.Next(&row)

		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		dados := proto.Dados{DatatypeId: row[0].(int64), DataMin: row[1].(float64), DataMax: row[2].(float64)}

		data_list.DataList = append(data_list.DataList, &dados)

		//fmt.Println(row)
		//fmt.Println(data_list)

	}

	//return data_list, nil
	return &proto.DataList{DataList: data_list.DataList}, nil

}

func main() {
	listener, err := net.Listen("tcp", ":4040")
	if err != nil {
		panic(err)
	}

	srv := grpc.NewServer()
	proto.RegisterAddServiceServer(srv, &server{})
	reflection.Register(srv)

	if e := srv.Serve(listener); e != nil {
		panic(e)
	}

}
