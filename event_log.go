/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"sort"
	"strconv"
	"time"
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
	if !ev.hasContent() {
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
