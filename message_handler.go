package crowd

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/vmware/transport-go/bridge"
	"github.com/vmware/transport-go/bus"
	"github.com/vmware/transport-go/model"
	"github.com/vogtp/go-hcl"
)

// Message defines the most basic event
type Message interface {
	Name() string
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
	wgMsg             sync.WaitGroup
	mu                sync.Mutex
	hcl               hcl.Logger
	bus               bus.EventBus
	handlers          []bus.MessageHandler
	bridge            bridge.Connection
	brokerDestination string
	channel           string
}

// NewMessageHandler creates a bew handler for events
func NewMessageHandler[M Message](channel string) *MessageHandler[M] {
	h := &MessageHandler[M]{
		channel:  channel,
		bus:      bus.GetBus(),
		handlers: make([]bus.MessageHandler, 0),
	}
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
	h.brokerDestination = "/topic/" + h.channel
	err := h.bus.GetChannelManager().MarkChannelAsGalactic(h.channel, h.brokerDestination, bridge)
	if err != nil {
		return fmt.Errorf("Could not mark channel %q as galactic: %w", h.channel, err)
	}
	h.bridge = bridge
	h.bridge.Subscribe(h.brokerDestination)
	h.hcl.Info("Connected to fabric")
	return nil
}

// SendSzenarioEvt sends a SzenarioEvtMsg
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
	h.hcl.Tracef("Sending %s msg %q", h.channel, (*evt).Name())
	if err := h.bus.SendBroadcastMessage(h.channel, evt); err != nil {
		return fmt.Errorf("could not send local message: %w", err)
	}
	// the following code panics in transport-go/bridge/bridge_client.go:188
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("could not marshal message: %w", err)
	}
	if err := h.bridge.SendJSONMessage(h.brokerDestination, payload); err != nil {
		return fmt.Errorf("could not send global message: %w", err)
	}
	return nil
}

// EventHandler handles events
type EventHandler[M Message] func(*M)

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
			h.hcl.Tracef("Got message: %+v", m)
			if evt, ok := m.Payload.(*M); ok {
				f(evt)
				return
			}
			evt := new(M)
			err := m.CastPayloadToType(evt)
			if err != nil {
				h.hcl.Errorf("Chan %q got unexpected message %v: %v", h.channel, m, err)
				return
			}
			f(evt)
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

func (h MessageHandler[M]) getChannel() string {
	return h.channel
}

func (h *MessageHandler[M]) cleanup() {
	h.WaitMsgProcessed()
	for _, h := range h.handlers {
		h.Close()
	}
	h.bus.GetChannelManager().MarkChannelAsLocal(h.channel)
}
