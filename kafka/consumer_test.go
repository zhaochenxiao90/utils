package kafka

import (
	"fmt"
	"testing"
	"time"

	sarama "gopkg.in/Shopify/sarama.v1"
)

func process(msg *sarama.ConsumerMessage) bool {
	fmt.Printf("topic:%s,key:%s\n", msg.Topic, msg.Key)
	return true
}

func TestInitKafkaConnect(t *testing.T) {
	conf := make(map[string]string)
	conf["broker"] = "10.110.101.27:9092"
	conf["consume_topic"] = "event_analysis_prepare"
	conf["consume_group"] = "test_consume"

	err := InitKafkaConnect(conf, false, true)
	if err != nil {
		t.Fatalf("InitKafkaConnect err:%s", err.Error())
	}
	GlobalKafkaConsumer.Start(10, process)
	time.Sleep(5 * time.Second)
	GlobalKafkaConsumer.Close()
}
