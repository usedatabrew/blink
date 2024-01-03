package service_registry

import (
	"context"
	"fmt"
	"github.com/charmbracelet/log"
	"github.com/usedatabrew/blink/config"
	"github.com/usedatabrew/blink/internal/stream_context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

var serviceKeyTemplate = "/service/%d"

type Registry struct {
	logger     *log.Logger
	ctx        *stream_context.Context
	etcdClient *clientv3.Client
	pipelineId int
	state      ServiceState
}

func NewServiceRegistry(ctx *stream_context.Context, config config.ETCD, pipelineId int) *Registry {
	registry := &Registry{
		logger:     ctx.Logger.WithPrefix("Service Registry"),
		ctx:        ctx,
		state:      Starting,
		pipelineId: pipelineId,
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{config.Host},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		registry.logger.Fatalf("failed to init etcd registry %s", err.Error())
	}
	registry.etcdClient = cli

	return registry
}

// Start runs goroutine that will set service key in the registry every 5 seconds
func (r *Registry) Start() {
	r.logger.Info("Starting ETCD Registry")
	go func() {
		for {
			ttl := 15

			// Create a lease with the desired TTL
			leaseResp, err := r.etcdClient.Grant(context.Background(), int64(ttl))
			if err != nil {
				fmt.Println(err)
				return
			}

			_, err = r.etcdClient.Put(context.Background(), fmt.Sprintf("/service/%d", r.pipelineId), string(r.state), clientv3.WithLease(leaseResp.ID))
			r.logger.Info("State update", "state", r.state)
			if err != nil {
				r.logger.Errorf("Failed to set the key into registry %s", err.Error())
				return
			}
			time.Sleep(time.Second * 10)
		}
	}()
}

func (r *Registry) SetState(state ServiceState) {
	r.logger.Info("Change pipeline state to", "state", state)
	r.state = state
}

func (r *Registry) Stop() {
	err := r.etcdClient.Close()
	if err != nil {
		r.logger.Fatal("Failed to gracefully close etcd registry conn")
	}
}
