package mqttc

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"

	"github.com/lithammer/shortuuid/v3"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	MQTTC_TOPIC     = "Topic"
	MQTTC_MESSAGEID = "MessageID"
	MQTTC_QOS       = "Qos"
	MQTTC_DUPLICATE = "Duplicate"
	MQTTC_RETAINED  = "Retained"
)

type Config struct {
	BrokerUrl string
	ClientID  string
}

type Mqttc struct {
	config Config
	logger watermill.LoggerAdapter

	closed     bool
	closedLock sync.Mutex
	closing    chan struct{}

	mqttco *mqtt.ClientOptions
	mqttc  mqtt.Client
}

func NewMqttc(config Config, logger watermill.LoggerAdapter) (*Mqttc, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	u, err := url.Parse(config.BrokerUrl)
	if err != nil {
		return nil, err
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%s", u.Hostname(), u.Port()))
	opts.SetClientID(config.ClientID)
	opts.SetUsername(u.User.Username())
	if pwd, has := u.User.Password(); has {
		opts.SetPassword(pwd)
	}
	// opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
	// 	if ok, sub := c.subscribers[msg.Topic()]; ok {
	// 		sub.outputChannel
	// 	}
	// 	fmt.Printf("TOPIC: %s\n", msg.Topic())
	// 	fmt.Printf("MSG: %s\n", string(msg.Payload()))
	// 	msg.Ack()

	// 	text := fmt.Sprintf("Publish msg %v", string(msg.Payload()))
	// 	token := client.Publish("bbbb", 1, false, text)
	// 	token.Wait()
	// })
	opts.OnConnect = func(client mqtt.Client) {
		optsr := client.OptionsReader()
		logger.Debug("OnConnect", watermill.LogFields{
			"Servers":  fmt.Sprintf("%v", optsr.Servers()),
			"ClientID": optsr.ClientID(),
		})
	}

	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		logger.Debug("OnConnectionLost", watermill.LogFields{
			"Error": err.Error(),
		})
	}
	opts.OnReconnecting = func(client mqtt.Client, opts *mqtt.ClientOptions) {
		logger.Debug("OnReconnecting", watermill.LogFields{
			"Servers":  fmt.Sprintf("%v", opts.Servers),
			"ClientID": opts.ClientID,
		})
	}

	c := mqtt.NewClient(opts)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return &Mqttc{
		config: config,

		logger: logger.With(watermill.LogFields{
			"pubsub_uuid": shortuuid.New(),
		}),

		closing: make(chan struct{}),

		mqttco: opts,
		mqttc:  c,
	}, nil
}

func (c *Mqttc) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		qos := byte(0)
		retained := false
		if v, err := strconv.ParseInt(msg.Metadata.Get(MQTTC_QOS), 10, 8); err == nil {
			qos = byte(v)
		}
		if v, err := strconv.ParseBool(msg.Metadata.Get(MQTTC_RETAINED)); err == nil {
			retained = v
		}

		if err := c.mqttc.Publish(topic, qos, retained, msg.Payload).Error(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Mqttc) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	messages := make(chan *message.Message)
	c.mqttc.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		submsg := message.NewMessage(watermill.NewUUID(), msg.Payload())
		submsg.Metadata.Set(MQTTC_TOPIC, msg.Topic())
		submsg.Metadata.Set(MQTTC_MESSAGEID, strconv.Itoa(int(msg.MessageID())))
		submsg.Metadata.Set(MQTTC_QOS, strconv.Itoa(int(msg.Qos())))
		submsg.Metadata.Set(MQTTC_DUPLICATE, strconv.FormatBool((msg.Duplicate())))
		submsg.Metadata.Set(MQTTC_RETAINED, strconv.FormatBool(msg.Retained()))
		messages <- submsg

		// logFields := watermill.LogFields{
		// 	"Topic":     msg.Topic(),
		// 	"MessageID": msg.MessageID(),
		// 	"Qos":       msg.Qos(),
		// 	"Retained":  msg.Retained(),
		// 	"Duplicate": msg.Duplicate(),
		// 	"Payload":   string(msg.Payload()),
		// }

		select {
		case <-submsg.Acked():
			// c.logger.Trace("Message acknowledged", logFields)
			msg.Ack()
		case <-submsg.Nacked():
			// c.logger.Trace("Message nacked", logFields)
		case <-ctx.Done():
			// c.logger.Info("Request stopped without ACK received", logFields)
		case <-c.closing:
			// c.logger.Info("Request stopped without ACK received", logFields)
		}
	})

	return messages, nil
}

func (c *Mqttc) Close() error {
	c.closedLock.Lock()
	defer c.closedLock.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	close(c.closing)

	c.logger.Debug("Closing Pub/Sub, waiting for subscribers", nil)

	c.logger.Info("Pub/Sub closed", nil)

	c.mqttc.Disconnect(250)
	return nil
}
