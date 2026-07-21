package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

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

func main() {
	mqURL := os.Getenv("RABBITMQ_URL")
	if mqURL == "" {
		mqURL = "amqp://guest:guest@localhost:5672/"
	}

	recipient := "estudiante@ucla.edu.ve"
	if len(os.Args) > 1 {
		recipient = os.Args[1]
	}

	log.Printf("Conectando a RabbitMQ/LavinMQ en %s...", mqURL)
	conn, err := amqp.Dial(mqURL)
	if err != nil {
		log.Fatalf("Error al conectar con LavinMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error al abrir canal: %v", err)
	}
	defer ch.Close()

	payload := EventPayload{
		EventType:      "USER_OTP_GENERATED",
		RecipientEmail: recipient,
		RecipientName:  "Usuario de Prueba UCLA",
		Data: OTPEventData{
			OTPCode:           "849201",
			ExpirationMinutes: 10,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Error al serializar JSON: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(
		ctx,
		"nexus_events",       // exchange
		"USER_OTP_GENERATED", // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		log.Fatalf("Error al publicar mensaje: %v", err)
	}

	fmt.Println("--------------------------------------------------")
	fmt.Println("✅ Evento de prueba publicado exitosamente en nexus_events:")
	fmt.Println(string(body))
	fmt.Println("--------------------------------------------------")
}
