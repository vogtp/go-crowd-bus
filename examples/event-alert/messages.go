package main

import (
	"fmt"
	"sync"
	"time"
)

var (
	TimeFormat = "02.01.2006 15:04:05"
)

// AlertMsg is a alert
type AlertMsg fullMsg

// EvtMsg stores information about a szenario run
type EvtMsg fullMsg

// GetAlert converts the Event to an Alert
func (s *EvtMsg) GetAlert() *AlertMsg {
	return &AlertMsg{
		baseEvtMsg:    s.baseEvtMsg,
		errorEvtMsg:   s.errorEvtMsg,
		counterEvtMsg: s.counterEvtMsg,
		statusEvtMsg:  s.statusEvtMsg,
	}
}

// NewSzenarioEvtMsg creates a SzenarioEvtMsg
func NewSzenarioEvtMsg(name string, now time.Time) *EvtMsg {
	e := &EvtMsg{
		baseEvtMsg: &baseEvtMsg{
			Iname: name,
			Itime: now,
		},
		errorEvtMsg:   &errorEvtMsg{Ierr: make([]errorMsgItem, 0)},
		counterEvtMsg: &counterEvtMsg{Icounters: make(map[string]any)},
		statusEvtMsg:  &statusEvtMsg{Istatuses: make(map[string]string)},
	}
	return e
}

// fullMsg contains all typicall fields
type fullMsg struct {
	*baseEvtMsg
	*errorEvtMsg
	*counterEvtMsg
	*statusEvtMsg
}

// baseEvtMsg conatins the basic message inforation used in all messages
type baseEvtMsg struct {
	Iname string
	Itime time.Time
}

// Name returns the (szenario) name
func (m baseEvtMsg) Name() string {
	return m.Iname
}

// Time of the event
func (m baseEvtMsg) Time() time.Time {
	return m.Itime
}

type errorMsgItem struct {
	Ierror error
	Itime  time.Time
}

// String retruns the error with time prefixed
func (e errorMsgItem) String() string {
	return fmt.Sprintf("%s %v", e.Itime.Format(TimeFormat), e.Ierror)
}

// Err return
func (e errorMsgItem) Err() error {
	return e.Ierror
}

// errorEvtMsg contains error information
type errorEvtMsg struct {
	Ierr []errorMsgItem
}

// AddErr adds a non nil error to the message
func (m *errorEvtMsg) AddErr(e error) {
	if e == nil {
		return
	}
	m.Ierr = append(m.Ierr, errorMsgItem{Ierror: e, Itime: time.Now()})
}

// Err returns nil if there are no error and the last non nil error otherwise
func (m *errorEvtMsg) Err() error {
	var err error
	for _, e := range m.Ierr {
		if e.Ierror != nil {
			err = e.Ierror
		}
	}
	return err
}

// Errs returns a slice of errors
func (m *errorEvtMsg) Errs() []errorMsgItem {
	return m.Ierr
}

// statusEvtMsg contains status information
type statusEvtMsg struct {
	mu        sync.RWMutex
	Istatuses map[string]string
}

// SetStatus sets a status
func (m *statusEvtMsg) SetStatus(key string, val string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Istatuses[key] = val
}

//Statuses returns all statuses
func (m *statusEvtMsg) Statuses() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Istatuses
}

// counterEvtMsg contains counter information
// a counter must be int or float
type counterEvtMsg struct {
	mu        sync.RWMutex
	Icounters map[string]any // it is ugly for go, but much nicer json since float and int are different
}

// SetCounter adds or replaces a counter
// a counter must be int or float
func (m *counterEvtMsg) SetCounter(key string, val any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Icounters[key] = val
}

// Counters returns all counters
func (m *counterEvtMsg) Counters() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Icounters
}