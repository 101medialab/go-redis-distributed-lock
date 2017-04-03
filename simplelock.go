package simplelock

import (
	"time"

	"github.com/go-redis/redis"
)

type LockManager struct {
	redisClient *redis.Client
}

func New(r *redis.Client) *LockManager {
	return &LockManager{redisClient: r}
}

//lockDuration = the total effective period for the lock
//waitTime = if the lock is already hold by someone, the period of time that current thread should wait for
func (m *LockManager) GetLock(name string, lockDuration, maxWaitTime time.Duration) (isSuccessful bool, token string) {
	temp, _ := time.Now().UTC().MarshalText()
	token = string(temp)

	parts := []float64{0.25, 0.2, 0.15, 0.1, 0.1, 0.1, 0.05, 0.05, 0} //the last one should always be zero

	for _, p := range parts {
		ok, err := m.redisClient.SetNX(name, token, lockDuration).Result()
		if err != nil {
			panic(err)
		}

		if ok {
			return true, token
		} else {
			if t := time.Duration(p * float64(maxWaitTime)); t >= 0 {
				time.Sleep(t)
			} else {
				//to allow quit exit if maxWaitTime = 0
				break
			}
		}
	}
	return false, ``
}

func (m *LockManager) ReleaseLock(name, token string) {
	//the lua script to perform atomic compare value and then delete
	//aim: check for the token, to avoid deleting the lock owned by others after one's lock expired
	script := `if redis.call('GET', KEYS[1]) == KEYS[2] then return redis.call('DEL', KEYS[1]) end return 0`

	if err := m.redisClient.Eval(script, []string{name, token}).Err(); err != nil {
		panic(err)
	}
}
