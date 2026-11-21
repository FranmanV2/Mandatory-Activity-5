package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/FranmanV2/Mandatory-Activity-5/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// How long to wait for one node before trying the next.
// Must be longer than the primary's replication timeout, because a backup
// forwards the request to the primary and waits for its answer.
const attemptTimeout = 5 * time.Second

type Client struct {
	id      string
	nodes   []string
	current int // index of the node that last worked
}

// call runs fn against the nodes, starting with the last one that worked,
// and moves on to the next node on error. Returns an error only if every
// node failed.
func (c *Client) call(fn func(ctx context.Context, cl pb.AuctionClient) error) error {
	var lastErr error
	for i := 0; i < len(c.nodes); i++ {
		idx := (c.current + i) % len(c.nodes)
		addr := c.nodes[idx]

		err := c.tryNode(addr, fn)
		if err == nil {
			c.current = idx
			return nil
		}
		fmt.Printf("  node %s failed (%v), trying next...\n", addr, err)
		lastErr = err
	}
	return fmt.Errorf("all nodes failed, last error: %w", lastErr)
}

func (c *Client) tryNode(addr string, fn func(ctx context.Context, cl pb.AuctionClient) error) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
	defer cancel()
	return fn(ctx, pb.NewAuctionClient(conn))
}

func (c *Client) bid(amount int32) {
	var outcome pb.Outcome
	err := c.call(func(ctx context.Context, cl pb.AuctionClient) error {
		reply, err := cl.Bid(ctx, &pb.BidRequest{BidderId: c.id, Amount: amount})
		if err != nil {
			return err
		}
		outcome = reply.Outcome
		return nil
	})
	if err != nil {
		fmt.Println("Bid could not be delivered:", err)
		return
	}
	fmt.Printf("Bid %d -> %s\n", amount, outcome)
}

func (c *Client) result() {
	var r *pb.ResultReply
	err := c.call(func(ctx context.Context, cl pb.AuctionClient) error {
		var err error
		r, err = cl.Result(ctx, &pb.ResultRequest{})
		return err
	})
	if err != nil {
		fmt.Println("Result could not be fetched:", err)
		return
	}
	if r.Over {
		fmt.Printf("Auction is OVER. Winner: %q with bid %d\n", r.HighestBidder, r.HighestBid)
	} else {
		fmt.Printf("Auction is running. Highest bid: %d by %q\n", r.HighestBid, r.HighestBidder)
	}
}

func main() {
	id := flag.String("id", "", "your bidder ID (required)")
	nodes := flag.String("nodes", "localhost:5001,localhost:5002,localhost:5003",
		"comma-separated list of node addresses")
	flag.Parse()

	if *id == "" {
		fmt.Println("Please provide a bidder ID, e.g. -id alice")
		os.Exit(1)
	}

	c := &Client{id: *id, nodes: strings.Split(*nodes, ",")}

	fmt.Printf("Bidder %q. Commands: bid <amount>, result, quit\n", c.id)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		parts := strings.Fields(scanner.Text())
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "bid":
			if len(parts) != 2 {
				fmt.Println("Usage: bid <amount>")
				continue
			}
			amount, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println("Amount must be a whole number")
				continue
			}
			c.bid(int32(amount))
		case "result":
			c.result()
		case "quit", "exit":
			return
		default:
			fmt.Println("Unknown command. Use: bid <amount>, result, quit")
		}
	}
}
