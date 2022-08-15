/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventLog(t *testing.T) {
	ev := &LocalEventLog{
		eventLog: make([]*Event, 2, 2),
		cap:      2,
		pointer:  0,
		sequence: 0,
	}
	testEvent := &Event{Data: []byte("test")}

	ev.Add("id", testEvent)
	ev.Clear("id")

	assert.Equal(t, 2, len(ev.eventLog))

	ev.Add("id", testEvent)
	ev.Add("id", testEvent)

	assert.Equal(t, 2, len(ev.eventLog))
}
