/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

// EventLog interface
type EventLog interface {
	Add(id string, ev *Event)
	Replay(id string, s *Subscriber)
	Clear(id string)
}

// EventLog holds all of previous events
type LocalEventLog struct {
	eventLog []*Event
	cap      int
	pointer  int
	sequence int
}

// Add event to eventlog
func (e *LocalEventLog) Add(id string, ev *Event) {
	if !ev.hasContent() || !ev.Save {
		return
	}

	ev.ID = []byte(strconv.Itoa(e.sequence))
	ev.timestamp = time.Now().UTC()
	e.sequence++
	e.eventLog[e.pointer] = ev
	e.pointer = (e.pointer + 1) % e.cap
	return
}

// Clear events from eventlog
func (e *LocalEventLog) Clear(id string) {
	e.eventLog = make([]*Event, e.cap, e.cap)
	return
}

// Replay events to a subscriber
func (e *LocalEventLog) Replay(id string, s *Subscriber) {
	sortedEventLog := []*Event{}

	for _, v := range e.eventLog {
		if v != nil {
			id, _ := strconv.Atoi(string(v.ID))
			if id >= s.eventid {
				sortedEventLog = append(sortedEventLog, v)
			}
		}
	}

	sort.Slice(sortedEventLog, func(i, j int) bool {
		idI, _ := strconv.Atoi(string(e.eventLog[i].ID))
		idJ, _ := strconv.Atoi(string(e.eventLog[j].ID))
		return idI < idJ
	})

	for _, v := range sortedEventLog {
		s.connection <- v
	}
	return
}

type redisRingBuffer struct {
	rs         *redis.Client
	cap        int64
	expiration time.Duration
}

func NewRedisRingBuffer(rs *redis.Client,
	cap int64,
	expiration time.Duration,
) *redisRingBuffer {
	return &redisRingBuffer{
		rs:         rs,
		cap:        cap,
		expiration: expiration,
	}
}

func (r *redisRingBuffer) Push(ctx context.Context, key string, element any) (err error) {
	elementJSON, err := json.Marshal(element)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}

	p := r.rs.Pipeline()
	p.LPush(ctx, key, elementJSON)
	p.LTrim(ctx, key, 0, r.cap)
	p.Expire(ctx, key, r.expiration)

	_, err = p.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to exec command: %w", err)
	}
	return
}

func (r *redisRingBuffer) Get(ctx context.Context, key string, element any) (err error) {
	result := r.rs.LRange(ctx, key, 0, r.cap)

	err = result.ScanSlice(element)
	if err != nil {
		return fmt.Errorf("failed to scan slice: %w", err)
	}
	return
}

func (r *redisRingBuffer) IncrementCounter(ctx context.Context, key string) (count int64) {
	return r.rs.Incr(ctx, key+"_counter").Val()
}

func (r *redisRingBuffer) Add(id string, ev *Event) {
	if !ev.hasContent() || !ev.Save {
		return
	}
	ev.timestamp = time.Now().UTC()
	ev.ID = []byte(strconv.Itoa(int(r.IncrementCounter(context.Background(), id))))

	r.Push(context.Background(), id, ev)
}

func (r *redisRingBuffer) Replay(id string, s *Subscriber) {
	sortedEventLog := []*Event{}

	err := r.Get(context.Background(), id, &sortedEventLog)
	if err != nil {
		return
	}

	for _, v := range sortedEventLog {
		if v != nil {
			id, _ := strconv.Atoi(string(v.ID))
			if id >= s.eventid {
				sortedEventLog = append(sortedEventLog, v)
			}
		}
	}

	sort.Slice(sortedEventLog, func(i, j int) bool {
		idI, _ := strconv.Atoi(string(sortedEventLog[i].ID))
		idJ, _ := strconv.Atoi(string(sortedEventLog[j].ID))
		return idI < idJ
	})

	for _, v := range sortedEventLog {
		s.connection <- v
	}
}

func (r *redisRingBuffer) Clear(id string) {
	r.rs.Del(context.Background(), id)
}
