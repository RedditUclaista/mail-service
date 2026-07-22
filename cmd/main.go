package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/RedditUclaista/mail-service/config"
	"github.com/RedditUclaista/mail-service/consumer"
	"github.com/RedditUclaista/mail-service/mailer"
)

func main() {
	log.Println("Initializing Nexus Mail Service...")

	cfg := config.LoadConfig()
	mailService := mailer.NewMailer(cfg)
	rabbitConsumer := consumer.NewRabbitConsumer(cfg, mailService)

	go rabbitConsumer.Start()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Println("Shutting down Nexus Mail Service gracefully...")
}
