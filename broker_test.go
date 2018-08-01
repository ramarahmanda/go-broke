package broke

import "testing"

func TestNewBroker(t *testing.T) {
	broker, err := NewBroker("memory")
	if err != nil {
		t.Fatalf("Failed creating new broker: %s", err)
	}

	if _, ok := broker.(*BrokerMemory); !ok {
		t.Fatalf("Expect BrokerMemory instance, got: %T", broker)
	}
}
