package db

type KvDB interface {
	Get(key string) (string, error)
	Set(key string, val string) error
}

type RedisDB struct{}

func (r *RedisDB) Get(key string) (string, error) {
	return "real redis get " + key, nil
}

func (r *RedisDB) Set(key string, val string) error {
	println("real redis set", key, val)
	return nil
}
