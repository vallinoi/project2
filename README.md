# Project 2

Services: OrderService (8000), MerchantService (8001), BuyerService (8002),
InventoryService (8003), PaymentService, EmailService. RabbitMQ events, Postgres per service.

## Run
1. Create 'EmailService/.env':

       SENDGRID_API_KEY=your_key
       SENDGRID_SENDER_EMAIL=your_verified_sender@example.com

2. Start everything:

       docker compose up --build --scale EmailService=2
