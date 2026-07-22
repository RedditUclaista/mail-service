package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	SMTPHost     string
	SMTPPort     string
	SMTPUsername string
	SMTPPassword string
	SMTPFromName string
	RabbitMQURL  string
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	return &Config{
		SMTPHost:     getEnv("SMTP_HOST", "smtp.gmail.com"),
		SMTPPort:     getEnv("SMTP_PORT", "587"),
		SMTPUsername: getEnv("SMTP_USERNAME", "nexus.soporte.app@gmail.com"),
		SMTPPassword: getEnv("SMTP_PASSWORD", "nexusapp1234"),
		SMTPFromName: getEnv("SMTP_FROM_NAME", "Nexus"),
		RabbitMQURL:  getEnv("RABBITMQ_URL", "amqp://guest:guest@lab3_mq:5672/"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}
