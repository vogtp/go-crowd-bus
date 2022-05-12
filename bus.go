package crowd

import (
	"fmt"
	"strings"
	"time"

	"github.com/vmware/transport-go/bridge"
	"github.com/vmware/transport-go/bus"
	"github.com/vogtp/go-hcl"
)

// Bus is the event bus
type Bus struct {
	hcl           hcl.Logger
	bus           bus.EventBus
	fabricAdr     string
	connectBroker bridge.Connection
	msgHandlers   map[string]messageHandler
}

// New creates a new eventbus
func New(opts ...Option) (evtBus *Bus, close func()) {
	e := &Bus{
		hcl:         hcl.New(),
		bus:         bus.GetBus(),
		msgHandlers: make(map[string]messageHandler),
	}
	for _, o := range opts {
		o(e)
	}

	return e, func() {
		for _, h := range e.msgHandlers {
			h.cleanup()
		}
		if e.connectBroker != nil {
			e.connectBroker.Disconnect()
		}
		if len(e.fabricAdr) > 0 {
			e.bus.StopFabricEndpoint()
		}
	}
}

func (e *Bus) AddMessageHandler(h messageHandler) error {
	h.init(e.hcl)
	if len(e.fabricAdr) > 0 {
		if err := h.markGalatic(e.getConnectBroker()); err != nil {
			e.hcl.Errorf(err.Error())
			return err
		}
	}
	e.msgHandlers[h.getChannel()] = h
	return nil
}

func (e Bus) WaitMsgProcessed() {
	for _, h := range e.msgHandlers {
		h.WaitMsgProcessed()
	}
}

func (e Bus) getConnectBroker() bridge.Connection {
	adr := e.fabricAdr
	if strings.HasPrefix(adr, ":") {
		adr = fmt.Sprintf("localhost%s", adr)
	}
	if e.connectBroker == nil {
		config := &bridge.BrokerConnectorConfig{
			Username:   "guest",
			Password:   "guest",
			ServerAddr: adr,
			WebSocketConfig: &bridge.WebSocketConfig{
				WSPath: "/ws",
				UseTLS: false,
			},
			UseWS:        true,
			HeartBeatOut: 30 * time.Second,
			// STOMPHeader:  map[string]string{},
			// HttpHeader: http.Header{
			// 	"Sec-Websocket-Protocol": {"v12.stomp"},
			// },
		}

		c, err := e.bus.ConnectBroker(config)
		if err != nil {
			hcl.Errorf("unable to connect to fabric, error: %e", err)
		}
		e.connectBroker = c
	}
	return e.connectBroker
}
