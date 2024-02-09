package server

import (
	"context"
	logpkg "github.com/vlamug/pdlog/internal/log"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vlamug/pdlog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"producer/consume stream succeeds":                   testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, teardown := setupTest(t)
			defer teardown()
			fn(t, client)
		})
	}
}

func setupTest(t *testing.T) (client api.LogClient, teardown func()) {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := logpkg.NewLog(dir, logpkg.Config{})
	require.NoError(t, err)

	srv, err := NewGRPCServer(clog)
	require.NoError(t, err)

	go func() {
		srv.Serve(l)
	}()

	client = api.NewLogClient(cc)

	return client, func() {
		srv.Stop()
		if err := cc.Close(); err != nil {
			log.Fatalln("failed to close grpc conn", err)
		}
		if err := clog.Remove(); err != nil {
			log.Fatalln("failed to remove logger", err)
		}
	}
}

func testProduceConsume(t *testing.T, client api.LogClient) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}
	produceResp, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)

	consumeResp, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produceResp.Offset,
	})
	require.NoError(t, err)

	require.Equal(t, want.Value, consumeResp.Record.Value)
	require.Equal(t, want.Offset, consumeResp.Record.Offset)
}

func testProduceConsumeStream(t *testing.T, client api.LogClient) {
	ctx := context.Background()

	produceResp, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{Value: []byte("hello world")},
		},
	)
	require.NoError(t, err)

	consumeResp, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produceResp.Offset + 1,
	})
	if consumeResp != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testConsumePastBoundary(t *testing.T, client api.LogClient) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{Value: record.Value, Offset: uint64(i)})
		}
	}
}
