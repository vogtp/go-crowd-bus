package crowd

import (
	"sync"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/vmware/transport-go/bus"
	"github.com/vogtp/go-hcl"
)

type A struct {
	A string
}

type B struct {
	*A
	B string
}

func TestLocalBus(t *testing.T) {
	hcl := hcl.New(hcl.WithLevel(hclog.Trace))
	bus, close := New(Hcl(hcl))
	defer close()

	handlerA := NewMessageHandler[A](bus, "A")
	handlerB := NewMessageHandler[B](bus, "B")
	var wg sync.WaitGroup

	cntA := 0
	cntB := 0
	handlerA.Handle(func(a *A) {
		cntA++
		wg.Done()
	})
	handlerB.Handle(func(b *B) {
		cntB++
		wg.Done()
	})

	wg.Add(1)
	handlerA.Send(&A{})
	wg.Wait()
	if cntA != 1 {
		t.Errorf("A should have 1 msg not %v", cntA)
	}
	if cntB != 0 {
		t.Errorf("B should have 0 msg not %v", cntB)
	}
}

func TestGlobalBus(t *testing.T) {
	hcl := hcl.New(hcl.WithLevel(hclog.Trace))
	bus1, close1 := New(FabricEndpoint(":54321"), Hcl(hcl.Named("bus1")))
	defer close1()

	bus.ResetBus()
	bus2, close2 := New(FabricEndpoint(":54321"), Hcl(hcl.Named("bus2")))
	defer close2()

	handler1A := NewMessageHandler[A](bus1, "A")
	handler1B := NewMessageHandler[B](bus1, "B")
	handler2A := NewMessageHandler[A](bus2, "A")
	var wg sync.WaitGroup

	cnt1A := 0
	cnt1B := 0
	handler1A.Handle(func(a *A) {
		cnt1A++
		hcl.Infof("Bus1 A%v: %+v", cnt1A, a)
		wg.Done()
	})
	handler1B.Handle(func(b *B) {
		cnt1B++
		wg.Done()
	})

	cnt2A := 0
	handler2A.Handle(func(a *A) {
		cnt2A++
		hcl.Infof("Bus2 A%v: %+v", cnt2A, a)
		wg.Done()
	})

	wg.Add(4)
	handler1A.Send(&A{"A"})
	wg.Wait()
	if cnt1A != 1 {
		t.Errorf("A should have 1 msg not %v", cnt1A)
	}
	if cnt1A != cnt2A {
		t.Errorf("Bus1 %v and bus2 %v did not get the same messages", cnt1A, cnt2A)
	}
	if cnt1B != 0 {
		t.Errorf("B should have 0 msg not %v", cnt1B)
	}
}
