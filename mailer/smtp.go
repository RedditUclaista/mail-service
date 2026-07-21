package mailer

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"net/smtp"
	"os"
	"path/filepath"

	"github.com/RedditUclaista/mail-service/config"
)

type Mailer struct {
	cfg *config.Config
}

type OTPData struct {
	RecipientName     string
	OTPCode           string
	ExpirationMinutes int
}

func NewMailer(cfg *config.Config) *Mailer {
	return &Mailer{cfg: cfg}
}

func (m *Mailer) SendOTPEmail(recipientEmail, recipientName, otpCode string, expirationMinutes int) error {
	tmplPath := filepath.Join("templates", "otp.html")
	if _, err := os.Stat(tmplPath); os.IsNotExist(err) {
		// Fallback for different working directory execution
		tmplPath = "otp.html"
	}

	tmpl, err := template.ParseFiles(tmplPath)
	if err != nil {
		return fmt.Errorf("failed to parse email template (%s): %w", tmplPath, err)
	}

	data := OTPData{
		RecipientName:     recipientName,
		OTPCode:           otpCode,
		ExpirationMinutes: expirationMinutes,
	}

	var body bytes.Buffer
	if err := tmpl.Execute(&body, data); err != nil {
		return fmt.Errorf("failed to execute email template: %w", err)
	}

	mime := "MIME-version: 1.0;\nContent-Type: text/html; charset=\"UTF-8\";\n\n"
	subject := "Subject: " + "Código de Verificación NEXUS" + "\n"
	from := fmt.Sprintf("From: %s <%s>\n", m.cfg.SMTPFromName, m.cfg.SMTPUsername)
	to := fmt.Sprintf("To: %s\n", recipientEmail)

	msg := []byte(from + to + subject + mime + body.String())

	auth := smtp.PlainAuth("", m.cfg.SMTPUsername, m.cfg.SMTPPassword, m.cfg.SMTPHost)
	addr := fmt.Sprintf("%s:%s", m.cfg.SMTPHost, m.cfg.SMTPPort)

	log.Printf("Sending OTP email to %s via SMTP (%s)...", recipientEmail, addr)

	err = smtp.SendMail(addr, auth, m.cfg.SMTPUsername, []string{recipientEmail}, msg)
	if err != nil {
		return fmt.Errorf("failed to send email via SMTP: %w", err)
	}

	log.Printf("Email successfully sent to %s", recipientEmail)
	return nil
}
