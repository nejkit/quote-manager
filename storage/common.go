package storage

import (
	"context"
	"quote-manager/errors"

	redis "github.com/redis/go-redis/v9"
)

type RedisClient struct {
	cli redis.Client
}

func NewRedisClient(connectionString string) RedisClient {
	client := redis.NewClient(&redis.Options{
		Addr: connectionString,
		DB:   0,
	})
	return RedisClient{
		cli: *client,
	}
}

func (c *RedisClient) SetKey(ctx context.Context, key string, value string) error {
	if err := c.cli.Set(ctx, key, value, 0); err.Err() != nil {
		return err.Err()
	}
	return nil
}

func (c *RedisClient) SetKeyNX(ctx context.Context, key string, value string) (bool, error) {
	result, err := c.cli.SetNX(ctx, key, value, 0).Result()
	if err != nil {
		return result, err
	}

	return result, nil
}

func (c RedisClient) DelKey(ctx context.Context, id string) {
	c.cli.Del(ctx, id)
}

func (c *RedisClient) GetKey(ctx context.Context, key string) (string, error) {
	result, err := c.cli.Get(ctx, key).Result()
	if err == redis.Nil {
		return result, errors.ErrorNotFound
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

func (c *RedisClient) DelKeyWithValue(ctx context.Context, key string, value string) error {
	identifier, err := c.cli.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	if identifier == value {
		_, err = c.cli.Del(ctx, key).Result()
	}
	if err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) GetKeysByPattern(ctx context.Context, pattern string) ([]string, error) {
	result, err := c.cli.Keys(ctx, pattern).Result()
	if err == redis.Nil {
		return result, errors.ErrorNotFound
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

func (c *RedisClient) GetFromHash(ctx context.Context, hashName string, value string) (*string, error) {
	info, err := c.cli.HGet(ctx, hashName, value).Result()
	if err == redis.Nil {
		return nil, errors.ErrorNotFound
	}
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func (c *RedisClient) GetAllFromHash(ctx context.Context, hashName string) ([]string, error) {
	infos, err := c.cli.HGetAll(ctx, hashName).Result()
	if err != nil {
		return nil, err
	}
	result := []string{}
	for _, v := range infos {
		result = append(result, v)
	}
	return result, nil

}

func (c *RedisClient) InsertHash(ctx context.Context, hashName string, key string, value []byte) error {
	state := c.cli.HSet(ctx, hashName, key, value)
	if state.Err() != nil {
		return state.Err()
	}
	return nil
}
