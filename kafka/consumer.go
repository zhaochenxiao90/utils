package kafka

import (
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	sarama "gopkg.in/Shopify/sarama.v1"
	cluster "gopkg.in/bsm/sarama-cluster.v2"
	"minerva/scloud/stargazer-base-lib/log"
)

type KafkaConsumer struct {
	servers     string
	topics      string
	group       string
	consumer    *cluster.Consumer
	client      *cluster.Client
	msggroup    map[string]chan *sarama.ConsumerMessage
	wg          sync.WaitGroup
	rateLimiter *rate.Limiter
	cancelFunc  context.CancelFunc
}

func NewKafkaConsumer(servers, topics, group string, userPwd ...string) (*KafkaConsumer, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	if len(userPwd) == 2 {
		if len(userPwd[0]) > 0 && len(userPwd[1]) > 0 {
			config.Net.SASL.Enable = true
			config.Net.SASL.User = userPwd[0]
			config.Net.SASL.Password = userPwd[1]
		}
	}

	// Init client
	topic_array := strings.Split(topics, ",")
	kafka_client, client_err := cluster.NewClient(strings.Split(servers, ","), config)
	if client_err != nil {
		return nil, client_err
	}

	//init consumer
	consumer, err := cluster.NewConsumerFromClient(kafka_client, group, topic_array)
	if err != nil {
		return nil, err
	}

	//new
	k_consumer := &KafkaConsumer{
		servers:  servers,
		topics:   topics,
		group:    group,
		consumer: consumer,
		client:   kafka_client,
		msggroup: make(map[string]chan *sarama.ConsumerMessage),
	}

	//每个topic
	for _, topic := range topic_array {
		partitionid_array, partition_err := kafka_client.Partitions(topic)
		if partition_err != nil {
			return nil, partition_err
		}

		//每个partition
		for _, partitionid := range partitionid_array {
			msggroup_id := topic + ":" + string(partitionid)
			k_consumer.msggroup[msggroup_id] = make(chan *sarama.ConsumerMessage, 50)
		}
	}

	k_consumer.getResponse()

	go func() {
		wait := make(chan os.Signal, 1)
		signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
		signal := <-wait

		log.Logger.Warn("get signal:%s!!!", signal.String())
		k_consumer.Close()
		log.Logger.Warn("kafka consumer closed!!!")
	}()

	return k_consumer, nil
}

func (k_consumer *KafkaConsumer) getResponse() {
	k_consumer.wg.Add(1)
	go func() {
		defer k_consumer.wg.Done()
		for err := range k_consumer.consumer.Errors() {
			log.Logger.Error("kafka consume error:%v", err)
		}
	}()

	k_consumer.wg.Add(1)
	go func() {
		defer k_consumer.wg.Done()
		for note := range k_consumer.consumer.Notifications() {
			log.Logger.Warn("kafka rebalanced:%v", note)
		}
	}()
}

func (k_consumer *KafkaConsumer) Close() {
	err := k_consumer.consumer.Close()
	if err != nil {
		log.Logger.Error("kafka close error:%v", err)
		return
	}
	k_consumer.cancelFunc()

	k_consumer.wg.Wait()
}

func (k_consumer *KafkaConsumer) WaitClosed() {
	k_consumer.wg.Wait()
}

func (k_consumer *KafkaConsumer) Start(rlimit int, execute func(*sarama.ConsumerMessage) bool) {
	//rateLimiter
	ctx, cancel := context.WithCancel(context.Background())
	r := rate.Limit(float64(rlimit))
	rlimiter := rate.NewLimiter(r, 1)

	k_consumer.cancelFunc = cancel
	k_consumer.rateLimiter = rlimiter

	go func() {
		var err error
		async_chan := make(chan bool, 100)
		for msg := range k_consumer.consumer.Messages() {
			err = k_consumer.rateLimiter.Wait(ctx)
			if err != nil {
				log.Logger.Error("rateLimiter wait error:%v", err)
				break
			}

			async_chan <- true
			go executeOne(async_chan, msg, execute)
			k_consumer.consumer.MarkOffset(msg, "")
		}
	}()
}

/*
func (k_consumer *KafkaConsumer) StartByPartition(rlimit int, execute func(*sarama.ConsumerMessage) bool) {
	//consumer
	go func() {
		for msg := range k_consumer.consumer.Messages() {
			msggroupid := msg.Topic + ":" + string(msg.Partition)
			msg_chan := k_consumer.msggroup[msggroupid]
			msg_chan <- msg
			k_consumer.consumer.MarkOffset(msg, "")
		}
	}()

	//handler
	for _, msg_chan := range k_consumer.msggroup {
		go func(msg_chan chan *sarama.ConsumerMessage) {
			async_chan := make(chan bool, rlimit)
			for msg := range msg_chan {
				async_chan <- true
				go executeOne(async_chan, msg, execute)
			}
		}(msg_chan)
	}
}

func (k_consumer *KafkaConsumer) StartSyncByPartition(execute func(*sarama.ConsumerMessage) bool) {
	//consumer
	go func() {
		for msg := range k_consumer.consumer.Messages() {
			msggroupid := msg.Topic + ":" + string(msg.Partition)
			msg_chan := k_consumer.msggroup[msggroupid]
			msg_chan <- msg
		}
	}()

	//handler
	for _, msg_chan := range k_consumer.msggroup {
		go func(msg_chan chan *sarama.ConsumerMessage) {
			for msg := range msg_chan {
				execute(msg)
				k_consumer.consumer.MarkOffset(msg, "")
			}
		}(msg_chan)
	}
}
*/

func executeOne(async_chan chan bool, msg *sarama.ConsumerMessage, execute func(*sarama.ConsumerMessage) bool) {
	defer func() {
		<-async_chan
	}()

	execute(msg)
}
