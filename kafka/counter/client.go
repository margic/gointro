package counter

import (
	"context"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/margic/gointro/kafka/protos"
	"google.golang.org/grpc"
)

// Client a counter grpc client
type Client struct {
	client protos.EventCountClient
}

// CountEvent calls count on the remote counter service
func (c *Client) CountEvent(content string) {
	in := &protos.EventIn{
		Content: content,
	}
	_, err := c.client.Count(context.Background(), in)
	if err != nil {
		log.WithError(err).Error("error sending message to counter")
	}
}

// NewClient creates a new grpc client
func NewClient() (*Client, error) {
	addr := viper.GetString("counter.address")
	log.WithField("address", addr).Info("starting counter client")
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := protos.NewEventCountClient(conn)
	c := &Client{
		client: client,
	}
	return c, nil
}
