package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/vogtp/go-crowd-bus"
	"github.com/vogtp/go-hcl"
)

var (
	end    chan bool
	listen = flag.Bool("l", false, "listen only")
)

func main() {
	flag.Parse()
	end = make(chan bool)
	hcl := hcl.New()
	bus := getBus(hcl)
	bus.Event.Handle(createLogger(hcl))
	bus.Event.Handle(createAlertMgr(bus))
	bus.Alert.Handle(createAlerter(hcl))

	if !*listen {
		go produceEvents(bus)
	}
	go func() {
		for {
			// make sure not all goroutines are asleep
			// pretent to work :)
			time.Sleep(time.Hour)
		}
	}()
	<-end
}

type EvtAlrtBus struct {
	bus   *crowd.Bus
	Event *crowd.MessageHandler[EvtMsg]   // Event is an abstraction for a event channel
	Alert *crowd.MessageHandler[AlertMsg] // Alert is an abstraction for a alert channel
}

// getBus initalises the bus and its channels
func getBus(hcl hcl.Logger) *EvtAlrtBus {
	b := &EvtAlrtBus{}
	bus, close := crowd.New(
		crowd.FabricEndpoint(":4444"),
		crowd.Hcl(hcl.Named("bus")),
	)
	defer close()
	b.bus = bus
	b.Event = crowd.NewMessageHandler[EvtMsg]("event")
	b.Alert = crowd.NewMessageHandler[AlertMsg]("alert")
	b.bus.AddMessageHandler(b.Event)
	b.bus.AddMessageHandler(b.Alert)
	return b
}

func produceEvents(bus *EvtAlrtBus) {
	ticker := time.NewTicker(5 * time.Second)
	i := 0
	for {
		name := fmt.Sprintf("Event%v", i)
		msg := NewSzenarioEvtMsg(name, time.Now())
		if rand.Intn(10) > 6 {
			msg.AddErr(errors.New("some random error"))
		}
		if err := bus.Event.Send(msg); err != nil {
			hcl.Errorf("Sending: %v", err)
		}
		i++
		<-ticker.C
	}
}

func createLogger(hcl hcl.Logger) crowd.EventHandler[EvtMsg] {
	hcl = hcl.Named("log")
	return func(e *EvtMsg) {
		hcl.Infof("Got Event %s", e.Name())
	}
}

func createAlertMgr(bus *EvtAlrtBus) crowd.EventHandler[EvtMsg] {
	return func(e *EvtMsg) {
		if e.Err() != nil {
			bus.Alert.Send(e.GetAlert())
		}
	}
}

func createAlerter(hcl hcl.Logger) crowd.EventHandler[AlertMsg] {
	hcl = hcl.Named("alert")
	return func(a *AlertMsg) {
		// generate a alert but just log it for now
		hcl.Errorf("Got Alert %s for %s", a.Err(), a.Name())
	}
}
