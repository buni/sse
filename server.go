/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"context"
	"encoding/base64"
	"sync"
	"time"
)

// DefaultBufferSize size of the queue that holds the streams messages.
const DefaultBufferSize = 1024

// Server Is our main struct
type Server struct {
	// Extra headers adding to the HTTP response to each client
	Headers map[string]string
	// Sets a ttl that prevents old events from being transmitted
	EventTTL time.Duration
	// Specifies the size of the message buffer for each stream
	BufferSize int
	// Encodes all data as base64
	EncodeBase64 bool
	// Splits an events data into multiple data: entries
	SplitData bool
	// Enables creation of a stream when a client connects
	AutoStream bool
	// Enables automatic replay for each new subscriber that connects
	AutoReplay bool

	// Specifies the function to run when client subscribe or un-subscribe
	OnSubscribe   func(streamID string, sub *Subscriber)
	OnUnsubscribe func(streamID string, sub *Subscriber)

	streams   map[string]*Stream
	muStreams sync.RWMutex
	OnEvent       func(ctx context.Context, streamID string, ev *Event)
	// Specifies the EventLog used for each new stream
	EventLog EventLog
}

// New will create a server and setup defaults
func New() *Server {
	return &Server{
		BufferSize: DefaultBufferSize,
		AutoStream: false,
		AutoReplay: true,
		streams:    make(map[string]*Stream),
		Headers:    map[string]string{},
	}
}

// NewWithCallback will create a server and setup defaults with callback function
func NewWithCallback(onSubscribe, onUnsubscribe func(streamID string, sub *Subscriber)) *Server {
	return &Server{
		BufferSize:    DefaultBufferSize,
		AutoStream:    false,
		AutoReplay:    true,
		streams:       make(map[string]*Stream),
		Headers:       map[string]string{},
		OnSubscribe:   onSubscribe,
		OnUnsubscribe: onUnsubscribe,
	}
}

// Close shuts down the server, closes all of the streams and connections
func (s *Server) Close() {
	s.muStreams.Lock()
	defer s.muStreams.Unlock()

	for id := range s.streams {
		s.streams[id].close()
		delete(s.streams, id)
	}
}

// CreateStream will create a new stream and register it
func (s *Server) CreateStream(id string, options ...StreamOption) *Stream {
	s.muStreams.Lock()
	defer s.muStreams.Unlock()

	if s.streams[id] != nil {
		return s.streams[id]
	}

	str := newStream(id, s.BufferSize, s.AutoReplay, s.AutoStream, s.OnSubscribe, s.OnUnsubscribe, s.EventLog)

	for _, option := range options {
		option(str)
	}

	str.run()

	s.streams[id] = str

	return str
}

// RemoveStream will remove a stream
func (s *Server) RemoveStream(id string) {
	s.muStreams.Lock()
	defer s.muStreams.Unlock()

	if s.streams[id] != nil {
		s.streams[id].close()
		delete(s.streams, id)
		s.streams[id].EventLog.Clear(id)
	}
}

// GracefullyRemoveStream will remove a stream if there are zero subscribers
// returns true if the streams has been closed
func (s *Server) GracefullyRemoveStream(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Streams[id] != nil {
		if s.Streams[id].GetSubscriberCount() == 0 {
			s.Streams[id].close()
			delete(s.Streams, id)
			return true
		}
	}
	return false
}

// StreamExists checks whether a stream by a given id exists
func (s *Server) StreamExists(id string) bool {
	return s.getStream(id) != nil
}

// Publish sends a mesage to every client in a streamID.
// If the stream's buffer is full, it blocks until the message is sent out to
// all subscribers (but not necessarily arrived the clients), or when the
// stream is closed.
func (s *Server) Publish(id string, event *Event) {
	stream := s.getStream(id)
	if stream == nil {
		return
	}

	select {
	case <-stream.quit:
	case stream.event <- s.process(event):
	}
}

// Publish sends a message to every client in a streamID
// if id is empty sends the message to all streams
func (s *Server) PublishAll(id string, event *Event) {
	s.muStreams.Lock()
	defer s.muStreams.Unlock()

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	if id == "" {
		for _, stream := range s.Streams {
			stream.event <- s.process(event)
		}
	}

	if s.streams[id] != nil {
		s.streams[id].event <- s.process(event)
	}
}

func (s *Server) getStream(id string) *Stream {
	s.muStreams.RLock()
	defer s.muStreams.RUnlock()
	return s.streams[id]
}

func (s *Server) process(event *Event) *Event {
	if s.EncodeBase64 {
		output := make([]byte, base64.StdEncoding.EncodedLen(len(event.Data)))
		base64.StdEncoding.Encode(output, event.Data)
		event.Data = output
	}
	return event
}
