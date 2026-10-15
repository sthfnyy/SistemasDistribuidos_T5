## Fluxo de funcionamento

O fluxo principal do sistema é:

Producer → RabbitMQ → Topic Exchange → Filas → Consumers

- `review.product` → `queue.reviews` → Sentiment Consumer
- `support.request` → `queue.support` → Support Consumer
- Mensagens com erro → `dead.letter.exchange` → `queue.dlq`

## Tecnologias utilizadas

- Java
- Docker
- Docker Compose
- RabbitMQ
- Smile 3.1.1

## Evidências e demonstração

As evidências de execução estão disponíveis na pasta `evidencias/`.

Vídeo de demonstração:

`https://youtu.be/MgPsY4_uhQk`