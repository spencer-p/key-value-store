package uuid

import (
	"strconv"
	"strings"
)

type UUID struct {
	IP   uint32
	Port uint16
	Seq  uint64
}

func New(addr string) UUID {
	var u UUID
	pieces := strings.Split(addr, ":")

	if len(pieces) >= 1 {
		ippieces := strings.Split(pieces[0], ".")
		for _, nstring := range ippieces {
			n, _ := strconv.ParseUint(nstring, 10, 8)
			u.IP <<= 8
			u.IP |= uint32(n)
		}
	}

	if len(pieces) >= 2 {
		port, _ := strconv.ParseUint(pieces[1], 10, 16)
		u.Port = uint16(port)
	}
	return u
}

func (u UUID) Next() (next UUID) {
	next = u
	next.Seq += 1
	return
}

func (u UUID) Greater(v UUID) bool {
	return u.Seq > v.Seq || u.IP > v.IP || u.Port > v.Port
}

func (u UUID) Equal(v UUID) bool {
	return u.Seq == v.Seq && u.IP == v.IP && u.Port == v.Port
}

func (u UUID) OriginatedOn(addr string) bool {
	v := New(addr)
	return u.IP == v.IP && u.Port == v.Port
}

func Ptr(u UUID) *UUID {
	return &u
}
