package causal

import (
	"net/http"
)

type DeliveryMan struct {
	msgbuf  []*msg
	current Clock
	get     func(*http.Request) Clock
	next    http.Handler
}

type msg struct {
	r *http.Request
	w http.ResponseWriter
	c Clock
}

func NewDeliveryMan(h http.Handler,
	initial Clock,
	get func(*http.Request) Clock,
	put func(*http.Request, Clock)) *DeliveryMan {

	return &DeliveryMan{
		msgbuf:  nil,
		current: initial,
		get:     get,
		next:    h,
	}
}

func (d *DeliveryMan) Receive(w http.ResponseWriter, r *http.Request) {
	d.add(&msg{
		r: r,
		w: w,
		c: d.get(r),
	})

	d.process()
}

func (d *DeliveryMan) add(m *msg) {
	// TODO(spencer-p) using a heap is better
	d.msgbuf = append(d.msgbuf, m)
}

func (d *DeliveryMan) process() {
	for didwork := true; didwork; {
		didwork = false
		for _, msg := range d.msgbuf {
			if msg.c.Less(d.current) {
				d.next.ServeHTTP(msg.w, msg.r)
				didwork = true
			}
		}
	}
}
