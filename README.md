# Mail Microservice

Este es el microservicio independiente de correo electrónico para la plataforma **Nexus** (red social estudiantil de la UCLA).

## ¿Qué hace?
- Funciona como consumidor de mensajes en LavinMQ (`email_queue`).
- Procesa eventos en segundo plano (como `USER_OTP_GENERATED`).
- Genera y envía correos transaccionales HTML utilizando plantillas dinámicas adaptadas a la identidad de marca de Nexus.

## Estructura del Proyecto
```
mail-service/
├── cmd/
│   └── main.go         # Punto de entrada principal
├── config/
│   └── config.go       # Carga de variables de entorno (.env)
├── consumer/
│   └── rabbitmq.go     # Consumidor AMQP / LavinMQ
├── mailer/
│   └── smtp.go         # Renderizado y envío de correos vía SMTP
├── templates/
│   └── otp.html        # Plantilla HTML con estilos institucional Nexus (UCLA)
├── .env.example
├── Dockerfile          # Dockerfile multi-stage
├── docker-compose.yml
└── go.mod
```

## ¿Cómo levantarlo?

1. Asegúrate de tener la red compartida y el Gateway/LavinMQ en ejecución:
   ```bash
   docker network create lab3_shared_network
   ```
2. Copia la plantilla de configuración de entorno (si es necesario):
   ```bash
   cp .env.example .env
   ```
3. Levanta el microservicio con Docker Compose:
   ```bash
   docker-compose up -d
   ```

## Estructura del Evento Esperado

El consumidor escucha en la cola `email_queue` vinculada al Exchange `nexus_events` payloads JSON como el siguiente:

```json
{
  "event_type": "USER_OTP_GENERATED",
  "recipient_email": "estudiante@ucla.edu.ve",
  "recipient_name": "Nombre Usuario",
  "data": {
    "otp_code": "482910",
    "expiration_minutes": 10
  }
}
```
