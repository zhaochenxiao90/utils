package kafkazk

import (
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
	"time"
)

func TestKafkaConsumer(t *testing.T) {
	kc := map[string]string{
		"broker":        "10.148.10.74:9092,10.127.92.99:9092",
		"zookeeper":     "10.127.92.99:2181",
		"consume_topic": "scloud.stargazer.tvlog",
		"consume_group": "gotest",
	}

	err := InitKafkaConnect(kc, false, true)
	if err != nil {
		t.Fatal(err)
	}

	GlobalKafkaConsumer.Start(10, MyTestHandler)
	time.Sleep(3 * time.Second)
	GlobalKafkaConsumer.Close()
}

func MyTestHandler(msg *sarama.ConsumerMessage) bool {
	fmt.Println(string(msg.Value))
	return true
}
