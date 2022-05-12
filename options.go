package crowd

import (
	"github.com/vmware/transport-go/bus"
	"github.com/vmware/transport-go/stompserver"
	"github.com/vogtp/go-hcl"
)

// Option configures the Engine
type Option func(*Bus)

// FabricEndpoint is an Option to start a fabric endpoint as goroutine
func Hcl(hcl hcl.Logger) Option {
	return func(e *Bus) {
		e.hcl = hcl
	}
}

// FabricEndpoint is an Option to start a fabric endpoint as goroutine
func FabricEndpoint(adr string) Option {
	return func(e *Bus) {
		e.fabricAdr = adr
		stompListener, err := stompserver.NewWebSocketConnectionListener(adr, "/ws", nil)
		if err != nil {
			e.hcl.Errorf("Cannot start stomp listener: %v", err)
			return
		}
		go func() {
			e.hcl.Infof("Stating fabric endpoint on %s", adr)
			err := e.bus.StartFabricEndpoint(stompListener, bus.EndpointConfig{
				TopicPrefix:           "/topic",
				UserQueuePrefix:       "/queue",
				AppRequestPrefix:      "/pub",
				AppRequestQueuePrefix: "/pub/queue",
				Heartbeat:             60000,
			})
			if err != nil {
				e.fabricAdr = ""
				e.hcl.Errorf("Cannot start endpoint: %v", err)
			}
		}()

	}
}
