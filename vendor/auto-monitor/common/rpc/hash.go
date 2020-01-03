package rpc

import (
	"hash/crc32"

	"github.com/spaolacci/murmur3"
)

func Crc32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func Murmur3(data []byte) uint32 {
	return murmur3.Sum32WithSeed(data, 0)
}
