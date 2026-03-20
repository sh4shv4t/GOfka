package broker

import (
	"os"
	"sync"
	"github.com/sh4shv4t/GOfka/store"
)

type Topic struct {
	TopicName   string
	LogFilePath string
	Lock        sync.RWMutex
}

func (t *Topic) Push(msg []byte) (int64, error) {

	t.Lock.Lock()
	defer t.Lock.Unlock()

	stats, err := os.Stat(t.LogFilePath)
	if err != nil {
		return 0, err
	}

	_, err = store.AppendMessage(t.LogFilePath, msg)
	if err != nil {
		return 0, err
	}

	return stats.Size(), nil
}

func (t *Topic) Pull(offset int64) ([]byte, error){
	t.Lock.RLock() //using readlock to allow multiple readers but only one writer at a time
	defer t.Lock.RUnlock()

	return store.ReadMessage(t.LogFilePath, offset)
}
