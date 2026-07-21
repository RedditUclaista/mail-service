package consumer

import (
	"encoding/json"
	"log"
	"time"

	"github.com/RedditUclaista/mail-service/config"
	"github.com/RedditUclaista/mail-service/mailer"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OTPEventData struct {
	OTPCode           string `json:"otp_code"`
	ExpirationMinutes int    `json:"expiration_minutes"`
}

type EventPayload struct {
	EventType      string       `json:"event_type"`
	RecipientEmail string       `json:"recipient_email"`
	RecipientName  string       `json:"recipient_name"`
	Data           OTPEventData `json:"data"`
}

type RabbitConsumer struct {
	cfg    *config.Config
	mailer *mailer.Mailer
}

func NewRabbitConsumer(cfg *config.Config, mailer *mailer.Mailer) *RabbitConsumer {
	return &RabbitConsumer{
		cfg:    cfg,
		mailer: mailer,
	}
}

func (c *RabbitConsumer) Start() {
	for {
		log.Printf("Connecting to LavinMQ/RabbitMQ at %s ...", c.cfg.RabbitMQURL)
		conn, err := amqp.Dial(c.cfg.RabbitMQURL)
		if err != nil {
			log.Printf("Failed to connect to RabbitMQ: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("Failed to open channel: %v", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		exchangeName := "nexus_events"
		queueName := "email_queue"

		// Declare topic exchange
		err = ch.ExchangeDeclare(
			exchangeName, // name
			"topic",      // type
			true,         // durable
			false,        // auto-deleted
			false,        // internal
			false,        // no-wait
			nil,          // arguments
		)
		if err != nil {
			log.Printf("Failed to declare exchange %s: %v", exchangeName, err)
			ch.Close()
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		// Declare queue
		q, err := ch.QueueDeclare(
			queueName, // name
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments
		)
		if err != nil {
			log.Printf("Failed to declare queue %s: %v", queueName, err)
			ch.Close()
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		// Bind queue to topic exchange
		routingKeys := []string{"USER_OTP_GENERATED", "email.#", "#"}
		for _, key := range routingKeys {
			err = ch.QueueBind(
				q.Name,       // queue name
				key,          // routing key
				exchangeName, // exchange
				false,
				nil,
			)
			if err != nil {
				log.Printf("Failed to bind queue with routing key %s: %v", key, err)
			}
		}

		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer tag
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			log.Printf("Failed to register a consumer: %v", err)
			ch.Close()
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("[*] Mail Service connected to LavinMQ. Listening on queue '%s' (Exchange: %s)...", q.Name, exchangeName)

		notifyClose := conn.NotifyClose(make(chan *amqp.Error))

		for {
			select {
			case errClose := <-notifyClose:
				log.Printf("RabbitMQ connection closed: %v. Reconnecting...", errClose)
				goto reconnect
			case d, ok := <-msgs:
				if !ok {
					log.Printf("Message channel closed. Reconnecting...")
					goto reconnect
				}

				c.handleDelivery(d)
			}
		}

	reconnect:
		ch.Close()
		conn.Close()
		time.Sleep(3 * time.Second)
	}
}

func (c *RabbitConsumer) handleDelivery(d amqp.Delivery) {
	log.Printf("Received message [RoutingKey: %s]: %s", d.RoutingKey, string(d.Body))

	var payload EventPayload
	if err := json.Unmarshal(d.Body, &payload); err != nil {
		log.Printf("Error unmarshaling event payload: %v", err)
		_ = d.Nack(false, false) // Reject invalid message
		return
	}

	switch payload.EventType {
	case "USER_OTP_GENERATED":
		err := c.mailer.SendOTPEmail(
			payload.RecipientEmail,
			payload.RecipientName,
			payload.Data.OTPCode,
			payload.Data.ExpirationMinutes,
		)
		if err != nil {
			log.Printf("Failed to send OTP email to %s: %v", payload.RecipientEmail, err)
			_ = d.Nack(false, true) // Requeue for retry
			return
		}
		log.Printf("Successfully processed and sent OTP email to %s", payload.RecipientEmail)
		_ = d.Ack(false)

	default:
		log.Printf("Received non-OTP event type (%s), acknowledging and ignoring.", payload.EventType)
		_ = d.Ack(false)
	}
}
