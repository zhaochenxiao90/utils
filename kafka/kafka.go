package kafka

import (
	"errors"
	"strings"
	"sync"
	"time"

	"gopkg.in/Shopify/sarama.v1"
	"minerva/scloud/stargazer-base-lib/log"
)

var (
	GlobalKafkaProducerTopic string
	GlobalKafkaProducer      *KafkaProducer
	GlobalKafkaConsumer      *KafkaConsumer
)

type KafkaProducer struct {
	LogInterval int
	Enqueued    int64
	SucCount    int64
	ErrCount    int64
	Sarama      sarama.AsyncProducer
	wg          sync.WaitGroup
	closed      bool
}

func InitKafkaConnect(kafka_conf map[string]string, producer_flag bool, consumer_flag bool) error {
	//broken
	broker, broker_ok := kafka_conf["broker"]
	if !broker_ok {
		return errors.New("pls set \"broker\" in kafka section!")
	}
	user, _ := kafka_conf["user"]
	pwd, _ := kafka_conf["pwd"]

	//produce_topic
	if producer_flag {
		var err error
		GlobalKafkaProducer, err = NewKafkaProducerWithLog(broker, 5, user, pwd)
		if err != nil {
			return err
		}

		topic, topic_ok := kafka_conf["produce_topic"]
		if !topic_ok {
			return errors.New("pls set \"produce_topic\" in kafka section!")
		}
		GlobalKafkaProducerTopic = topic
	}

	//consumer
	if consumer_flag {
		//consume_topic
		topic, topic_ok := kafka_conf["consume_topic"]
		if !topic_ok {
			return errors.New("pls set \"consume_topic\" in kafka section!")
		}

		//consume_group
		group, group_ok2 := kafka_conf["consume_group"]
		if !group_ok2 {
			return errors.New("pls set \"consume_group\" in kafka section!")
		}

		//New Consumer
		var consumer_err error
		GlobalKafkaConsumer, consumer_err = NewKafkaConsumer(broker, topic, group, user, pwd, kafka_conf["offset"])
		if consumer_err != nil {
			return consumer_err
		}
	}

	return nil
}

func NewKafkaProducer(servers string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	server_array := strings.Split(servers, ",")
	producer, err := sarama.NewAsyncProducer(server_array, config)
	if err != nil {
		return nil, err
	}

	k_producer := &KafkaProducer{Sarama: producer}
	go k_producer.getResponse()

	return k_producer, nil
}

func NewKafkaProducerWithLog(servers string, interval int, userPwd ...string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	if len(userPwd) == 2 {
		if len(userPwd[0]) > 0 && len(userPwd[1]) > 0 {
			config.Net.SASL.Enable = true
			config.Net.SASL.User = userPwd[0]
			config.Net.SASL.Password = userPwd[1]
		}
	}

	server_array := strings.Split(servers, ",")
	producer, err := sarama.NewAsyncProducer(server_array, config)
	if err != nil {
		return nil, err
	}

	k_producer := &KafkaProducer{LogInterval: interval, Sarama: producer}
	if interval > 0 {
		go k_producer.Printlog()
	}
	go k_producer.getResponse()

	return k_producer, nil
}

func (producer *KafkaProducer) Send(topic, key string, content []byte) {
	producer.Sarama.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(string(content)),
	}
	producer.Enqueued++
}

func (producer *KafkaProducer) Printlog() {
	for {
		time.Sleep(time.Duration(producer.LogInterval) * time.Second)
		producer.print()
		if producer.closed {
			break
		}
	}
}

func (producer *KafkaProducer) print() {
	succount := producer.SucCount
	enqueued := producer.Enqueued
	errorcount := producer.ErrCount

	if enqueued > 0 || errorcount > 0 {
		//fmt.Printf("kafka enqueued: %d, errors: %d\n", enqueued, errorcount)
		log.Logger.Info("kafka enqueued: %d, errors: %d", enqueued, errorcount)
		producer.Enqueued = producer.Enqueued - enqueued
		producer.ErrCount = producer.ErrCount - errorcount
		producer.SucCount = producer.SucCount - succount
	}
}

func (producer *KafkaProducer) getResponse() {
	producer.wg.Add(1)
	go func() {
		defer producer.wg.Done()
		for err := range producer.Sarama.Errors() {
			producer.ErrCount++
			log.Logger.Error(
				"kafka error:%s", err.Error(),
			)
		}
	}()

	producer.wg.Add(1)
	go func() {
		defer producer.wg.Done()
		for _ = range producer.Sarama.Successes() {
			producer.SucCount++
		}
	}()

}

func (producer *KafkaProducer) Close() {
	producer.Sarama.AsyncClose()
	producer.waitCompleted()

	if producer.LogInterval > 0 {
		producer.print()
	}
}

func (producer *KafkaProducer) waitCompleted() {
	producer.closed = true
	producer.wg.Wait()
}
