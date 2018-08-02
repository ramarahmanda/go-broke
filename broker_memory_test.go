package broke

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"
)

func TestNewBrokerMemory(t *testing.T) {
	b, err := NewBrokerMemory()
	if err != nil {
		t.Fatalf("error when creating new memory broker: %s", err)
	}

	numMsgPerTopic := 10

	output := captureOutput(func() {
		topics := []string{"topic1", "topic2"}
		for _, topic := range topics {
			b.Subscribe(topic, func(msg interface{}) error {
				bytesMsg, ok := msg.([]byte)
				if !ok {
					t.Fatalf("Expect message type to []byte, got: %T", msg)
				}
				fmt.Println("Incoming message:", string(bytesMsg))
				return nil
			})
		}

		for _, topic := range topics {
			items := make([]int, numMsgPerTopic)
			for i := range items {
				go b.Publish(topic, "Message #"+strconv.Itoa(i+1)+" from "+topic)
			}
		}

		select {
		case <-time.After(1 * time.Second):
		}

	})

	if match := len(regexp.MustCompile("Message #[0-9]+ from topic2").FindAllStringIndex(output, -1)); match != numMsgPerTopic {
		t.Fatalf("Expect number of message of topic2 to %d, got: %d", numMsgPerTopic, match)
	}

	if match := len(regexp.MustCompile("Message #[0-9]+ from topic1").FindAllStringIndex(output, -1)); match != numMsgPerTopic {
		t.Fatalf("Expect number of message of topic2 to %d, got: %d", numMsgPerTopic, match)
	}

}

func captureOutput(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}
