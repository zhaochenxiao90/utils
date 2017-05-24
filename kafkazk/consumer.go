package kafkazk

import (
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	//"minerva/scloud/stargazer/util"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type KafkaConsumer struct {
	servers     string
	topics      string
	group       string
	consumer    *consumergroup.ConsumerGroup
	msggroup    map[string]chan *sarama.ConsumerMessage
	wg          sync.WaitGroup
	rateLimiter *rate.Limiter
	cancelFunc  context.CancelFunc
}

func NewKafkaConsumer(servers, topics, group, zk string) (*KafkaConsumer, error) {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	var zookeeperNodes []string
	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zk)

	// Init client
	topic_array := strings.Split(topics, ",")

	//init consumer
	consumer, consumerErr := consumergroup.JoinConsumerGroup(group, topic_array, zookeeperNodes, config)
	if consumerErr != nil {
		return nil, consumerErr
	}

	//new
	k_consumer := &KafkaConsumer{
		servers:  servers,
		topics:   topics,
		group:    group,
		consumer: consumer,
		msggroup: make(map[string]chan *sarama.ConsumerMessage),
	}

	k_consumer.getResponse()

	go func() {
		wait := make(chan os.Signal, 1)
		signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
		<-wait

		//util.Logger.Warn("get signal:%s!!!", signal.String())
		k_consumer.Close()
		//util.Logger.Warn("kafka consumer closed!!!")
	}()

	return k_consumer, nil
}

func (k_consumer *KafkaConsumer) getResponse() {
	k_consumer.wg.Add(1)
	go func() {
		defer k_consumer.wg.Done()
		for _ = range k_consumer.consumer.Errors() {
			//util.Logger.Error("kafka consume error:%v", err)
		}
	}()
}

func (k_consumer *KafkaConsumer) Close() {
	err := k_consumer.consumer.Close()
	if err != nil {
		//util.Logger.Error("kafka close error:%v", err)
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
				//util.Logger.Error("rateLimiter wait error:%v", err)
				break
			}

			async_chan <- true
			go executeOne(async_chan, msg, execute)
			k_consumer.consumer.CommitUpto(msg)
		}
	}()
}

func executeOne(async_chan chan bool, msg *sarama.ConsumerMessage, execute func(*sarama.ConsumerMessage) bool) {
	defer func() {
		<-async_chan
	}()

	execute(msg)
}
