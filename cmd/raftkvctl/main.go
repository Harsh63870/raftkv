// raftkvctl is a thin client for driving and inspecting a raftkv cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/yourname/raftkv/internal/api"
)

const usage = `usage: raftkvctl [-addr host:port] <command>

  get <key>
  set <key> <value>
  del <key>
  status
  leader
  snapshot
  bench [-n ops] [-c concurrency] [-read]
`

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "node gRPC address")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call timeout")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	cc, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		die("dial %s: %v", *addr, err)
	}
	defer cc.Close()
	kv := api.NewKVClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch cmd, rest := args[0], args[1:]; cmd {
	case "get":
		need(rest, 1, "get <key>")
		res, err := kv.Get(ctx, &api.GetRequest{Key: rest[0]})
		check(err)
		if !res.Found {
			fmt.Fprintf(os.Stderr, "key not found (served by %s)\n", res.ServedBy)
			os.Exit(1)
		}
		fmt.Println(res.Value)

	case "set":
		need(rest, 2, "set <key> <value>")
		res, err := kv.Set(ctx, &api.SetRequest{Key: rest[0], Value: rest[1]})
		check(err)
		fmt.Println(ack(res.ForwardedTo))

	case "del":
		need(rest, 1, "del <key>")
		res, err := kv.Delete(ctx, &api.DeleteRequest{Key: rest[0]})
		check(err)
		fmt.Println(ack(res.ForwardedTo))

	case "status":
		res, err := kv.Status(ctx, &api.StatusRequest{})
		check(err)
		b, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(res)
		check(err)
		fmt.Println(string(b))

	case "leader":
		res, err := kv.Status(ctx, &api.StatusRequest{})
		check(err)
		if res.LeaderId == "" {
			die("no leader")
		}
		fmt.Printf("%s %s\n", res.LeaderId, res.LeaderAddr)

	case "snapshot":
		res, err := kv.Snapshot(ctx, &api.SnapshotRequest{})
		check(err)
		fmt.Printf("snapshot at index %d\n", res.Index)

	case "bench":
		bench(kv, rest)

	default:
		flag.Usage()
		os.Exit(2)
	}
}

func bench(kv api.KVClient, args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("n", 2000, "total operations")
	c := fs.Int("c", 16, "concurrent workers")
	read := fs.Bool("read", false, "benchmark reads instead of writes")
	fs.Parse(args)

	var ok, failed atomic.Int64
	work := make(chan int, *c)
	start := time.Now()

	var wg sync.WaitGroup
	for range *c {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				key := "bench/" + strconv.Itoa(i)
				var err error
				if *read {
					_, err = kv.Get(ctx, &api.GetRequest{Key: key})
				} else {
					_, err = kv.Set(ctx, &api.SetRequest{Key: key, Value: strconv.Itoa(i)})
				}
				cancel()
				if err != nil {
					failed.Add(1)
					continue
				}
				ok.Add(1)
			}
		}()
	}
	for i := range *n {
		work <- i
	}
	close(work)
	wg.Wait()

	elapsed := time.Since(start)
	mode := "write"
	if *read {
		mode = "read"
	}
	fmt.Printf("%s: %d ops in %s (%.0f ops/sec), %d failed\n",
		mode, ok.Load(), elapsed.Round(time.Millisecond),
		float64(ok.Load())/elapsed.Seconds(), failed.Load())
	if failed.Load() > 0 {
		os.Exit(1)
	}
}

func ack(forwardedTo string) string {
	if forwardedTo == "" {
		return "OK"
	}
	return "OK (forwarded to " + forwardedTo + ")"
}

func need(args []string, n int, form string) {
	if len(args) != n {
		die("usage: %s", form)
	}
}

func check(err error) {
	if err != nil {
		die("%v", err)
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
