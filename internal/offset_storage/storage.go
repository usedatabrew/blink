package offset_storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net/url"
)

type Storage struct {
	redisCache *redis.Client
}

func NewOffsetStorage(storageURI string) OffsetStorage {
	parsedRedisUrl, err := url.Parse(storageURI)
	if err != nil {
		panic(err)
	}

	passwd, _ := parsedRedisUrl.User.Password()
	op := &redis.Options{Addr: parsedRedisUrl.Host, Password: passwd, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	fmt.Println(parsedRedisUrl.Host, passwd)
	client := redis.NewClient(op)

	return &Storage{
		redisCache: client,
	}
}

func (o *Storage) GetOffsetByPipelineStream(key string) (int64, error) {
	resCmd := o.redisCache.Get(context.Background(), key)
	return resCmd.Int64()
}

func (o *Storage) SetOffsetForPipeline(key string, offset int64) error {
	cmd := o.redisCache.Set(context.Background(), key, offset, 0)
	return cmd.Err()
}
