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
	// Format 1: Direct JSON payload
	EventType      string       `json:"event_type"`
	RecipientEmail string       `json:"recipient_email"`
	RecipientName  string       `json:"recipient_name"`
	Data           OTPEventData `json:"data"`

	// Format 2: Outbox relay payload from auth-service
	Email   string `json:"email"`
	OTPCode string `json:"otp_code"`
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

		queueName := "email_queue"
		exchanges := []string{"nexus_events", "topic"}

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

		// Declare and bind exchanges
		routingKeys := []string{"USER_OTP_GENERATED", "core.otp.request", "email.#", "#"}
		for _, exName := range exchanges {
			err = ch.ExchangeDeclare(
				exName, // name
				"topic", // type
				true,    // durable
				false,   // auto-deleted
				false,   // internal
				false,   // no-wait
				nil,     // arguments
			)
			if err != nil {
				log.Printf("Warning: Exchange %s declare: %v", exName, err)
			}

			for _, key := range routingKeys {
				err = ch.QueueBind(
					q.Name, // queue name
					key,    // routing key
					exName, // exchange
					false,
					nil,
				)
				if err != nil {
					log.Printf("Failed to bind queue %s to exchange %s with key %s: %v", q.Name, exName, key, err)
				}
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
			log.Printf("Failed to register consumer: %v", err)
			ch.Close()
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("[*] Mail Service connected to LavinMQ. Listening on queue '%s'...", q.Name)

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
		_ = d.Nack(false, false)
		return
	}

	// Extract email and OTP code from either format
	recipientEmail := payload.RecipientEmail
	if recipientEmail == "" {
		recipientEmail = payload.Email
	}

	recipientName := payload.RecipientName
	if recipientName == "" {
		recipientName = "Usuario Nexus"
	}

	otpCode := payload.Data.OTPCode
	if otpCode == "" {
		otpCode = payload.OTPCode
	}

	expirationMinutes := payload.Data.ExpirationMinutes
	if expirationMinutes == 0 {
		expirationMinutes = 5
	}

	// If it's an OTP request or contains OTP code
	if otpCode != "" && recipientEmail != "" {
		err := c.mailer.SendOTPEmail(
			recipientEmail,
			recipientName,
			otpCode,
			expirationMinutes,
		)
		if err != nil {
			log.Printf("Failed to send OTP email to %s: %v", recipientEmail, err)
			time.Sleep(5 * time.Second)
			_ = d.Nack(false, true)
			return
		}
		log.Printf("Successfully processed and sent OTP email to %s (Code: %s)", recipientEmail, otpCode)
		_ = d.Ack(false)
		return
	}

	log.Printf("Acknowledged non-OTP message [RoutingKey: %s]", d.RoutingKey)
	_ = d.Ack(false)
}
