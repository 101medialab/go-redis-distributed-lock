package redisLock

import (
	"time"

	"github.com/go-redis/redis"
)

type LockFactory struct {
	RedisClient *redis.Client  `inject:""`
}

func New(r *redis.Client) *LockFactory {
	return &LockFactory{r}
}

type LockOptions struct {
	LockDuration time.Duration
	MaxWaitTime  time.Duration
}

func (m *LockFactory) Lock(name string, options *LockOptions) *Lock {
	if options == nil {
		options = &LockOptions{
			time.Duration(5) * time.Second,
			time.Duration(5) * time.Second + time.Duration(100) * time.Millisecond,
		}
	}

	temp, _ := time.Now().UTC().MarshalText()
	token := string(temp)

	// Recessive checking
	for _, p := range []float32{0.25, 0.2, 0.15, 0.1, 0.1, 0.1, 0.05, 0.05, 0} {
		ok, err := m.RedisClient.SetNX(name, token, options.LockDuration).Result()
		if err != nil {
			panic(err)
		}

		if ok {
			return &Lock{
				m.RedisClient,
				name,
				token,
			}
		} else {
			if t := time.Duration(p * float32(options.MaxWaitTime)); t > 0 {
				time.Sleep(t)
			} else {
				break
			}
		}
	}

	return nil
}

type Lock struct {
	redisClient *redis.Client
	name string
	token string
}

func (l *Lock) GetName() string {
	return l.name
}

// Release if you are owing the lock by checking the token of the lock stored in Redis is the same or not
func (l *Lock) Release() {
	err := l.redisClient.Eval(`
		if redis.call('GET', KEYS[1]) == KEYS[2] then 
			return redis.call('DEL', KEYS[1]) 
		end 
		
		return 0
	`,
		[]string{
			l.name,
			l.token,
		},
	).Err()

	if err != nil {
		panic(err)
	}
}
