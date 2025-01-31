package util

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	lock    = sync.Mutex{}                                // 互斥锁，确保在并发环境下对 randStr 的访问是安全的
	randStr = rand.New(rand.NewSource(time.Now().Unix())) // 伪随机数生成器
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

func GetTestKey(i int64) []byte {
	return []byte(fmt.Sprintf("lotusdb-test-key-%09d", i))
}

func RandomValue(n int) []byte {
	b := make([]byte, n)
	lock.Lock()
	defer lock.Unlock()
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("lotusdb-test-value-" + string(b))
}
