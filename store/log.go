package store

import (
	"encoding/binary"
	"io"
	"os"
)

func AppendMessage(filename string, msg []byte) (int, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	buf := make([]byte, 4+len(msg))

	binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))

	copy(buf[4:], msg)

	// ONE single write to the disk is much safer and faster
	n, err := file.Write(buf)
	if err != nil {
		return 0, err
	}

	return n, nil //we return nil to indicate that the message was successfully written
} // so returning nil is like a success indicator

func ReadMessage(filename string, offset int64) ([]byte, error) {

	file, err := os.OpenFile(filename, os.O_RDONLY, 0666)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	_, err = file.Seek(offset, 0)
	// fmt.Printf("New offset: %d\n", newOffset)

	if err != nil {
		return nil, err
	}

	buffer := make([]byte, 4)

	_, err = io.ReadFull(file, buffer)

	if err != nil {
		return nil, err
	}

	msgLength := binary.BigEndian.Uint32(buffer)
	msgBuffer := make([]byte, msgLength)

	_, err = io.ReadFull(file, msgBuffer)

	if err != nil {
		return nil, err
	}

	return msgBuffer, nil

	//used ReadFull instead of Read as Read is lazy and oftentimes allows OS to return values less than needed
	// while ReadFull ensure exact number of bytes needed are read
}
