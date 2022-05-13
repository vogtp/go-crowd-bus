package crowd

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vmware/transport-go/bridge"
	"github.com/vmware/transport-go/bus"
	"github.com/vmware/transport-go/model"
	"github.com/vogtp/go-hcl"
)

const (
	topic = "/topic/"
)

// Message defines the most basic event
type Message interface {
}

type messageHandler interface {
	WaitMsgProcessed()
	init(hcl.Logger)
	markGalatic(bridge.Connection) error
	getChannel() string
	cleanup()
}

// MessageHandler is a generic abstraction of Send and Handle
type MessageHandler[M Message] struct {
	id                uuid.UUID
	wgMsg             sync.WaitGroup
	mu                sync.Mutex
	hcl               hcl.Logger
	bus               bus.EventBus
	handlers          []bus.MessageHandler
	bridge            bridge.Connection
	brokerDestination string
	channel           string
}

type payload[M Message] struct {
	Event  *M
	Source uuid.UUID
}

// NewMessageHandler creates a bew handler for events
func NewMessageHandler[M Message](b *Bus, channel string) *MessageHandler[M] {
	h := &MessageHandler[M]{
		id:       uuid.New(),
		channel:  channel,
		bus:      b.bus,
		handlers: make([]bus.MessageHandler, 0),
	}
	b.addMessageHandler(h)
	return h
}

func (h *MessageHandler[M]) init(hcl hcl.Logger) {
	h.hcl = hcl.Named(h.channel)
	if !h.bus.GetChannelManager().CheckChannelExists(h.channel) {
		h.bus.GetChannelManager().CreateChannel(h.channel)
		hcl.Infof("Created channel %s", h.channel)
	}
}

func (h *MessageHandler[M]) markGalatic(bridge bridge.Connection) error {
	h.brokerDestination = topic + h.channel
	err := h.bus.GetChannelManager().MarkChannelAsGalactic(h.channel, h.brokerDestination, bridge)
	if err != nil {
		return fmt.Errorf("Could not mark channel %q as galactic: %w", h.channel, err)
	}
	h.bridge = bridge
	h.bridge.Subscribe(h.brokerDestination)
	h.bridge.SubscribeReplyDestination(h.brokerDestination)
	h.hcl.Infof("Connected to fabric: %v", h.brokerDestination)
	return nil
}

// Send messages
func (h *MessageHandler[M]) Send(evt *M) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while sending: %v", r)
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.wgMsg.Add(1)
	defer time.AfterFunc(500*time.Millisecond, h.wgMsg.Done)
	h.hcl.Debugf("Sending %s msg %+v", h.channel, *evt)
	msg := &payload[M]{
		Source: h.id,
		Event:  evt,
	}
	if err := h.bus.SendBroadcastMessage(h.channel, msg); err != nil {
		return fmt.Errorf("could not send local message: %w", err)
	}
	return nil
}

// EventHandler handles events
type EventHandler[M Message] func(*M)

func (h *MessageHandler[M]) getPayload(m *model.Message) (msg *payload[M], err error) {
	if evt, ok := m.Payload.(*payload[M]); ok {
		return evt, nil
	}
	msg = new(payload[M])
	if u, ok := m.Payload.([]uint8); ok {
		b := []byte(u)
		h.hcl.Tracef("unit cast: %v", string(b))
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in get payload from message: %v", r)
			}
		}()
		err = json.Unmarshal(b, msg)
		return msg, err
	}
	err = m.CastPayloadToType(msg)
	return msg, err
}

// HandleSzenarioEvt handles SzenarioEvtMsgs
func (h *MessageHandler[M]) Handle(f EventHandler[M]) error {
	handler, err := h.bus.ListenStream(h.channel)
	if err != nil {
		h.hcl.Errorf("Cannot register handler for chan %s: %v", h.channel, err)
		return err
	}
	h.handlers = append(h.handlers, handler)
	handler.Handle(
		func(m *model.Message) {
			h.wgMsg.Add(1)
			defer h.wgMsg.Done()
			h.hcl.Debugf("Got message: %+v", m)
			msg, err := h.getPayload(m)
			if err != nil {
				h.hcl.Errorf("Chan %q got unexpected message %v: %v", h.channel, m, err)
				return
			}
			if len(m.Destination) > 0 {
				h.hcl.Debugf("Message is from broker: %v", m.Destination)
				if msg.Source == h.id {
					h.hcl.Debugf("Got my own message: %+v", msg)
					return
				}
			}
			f(msg.Event)
		},
		func(err error) {
			h.hcl.Errorf("evt chan %s reported error: %v", h.channel, err)
		},
	)
	return nil
}

func (h *MessageHandler[M]) WaitMsgProcessed() {
	h.wgMsg.Wait()
}

func (h *MessageHandler[M]) getChannel() string {
	return h.channel
}

func (h *MessageHandler[M]) cleanup() {
	h.WaitMsgProcessed()
	for _, h := range h.handlers {
		h.Close()
	}
	h.bus.GetChannelManager().MarkChannelAsLocal(h.channel)
}
