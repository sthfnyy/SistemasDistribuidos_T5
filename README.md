# SistemasDistribuidos_T5

## Objetivo
Preparar a estrutura inicial de um sistema distribuído de processamento de mensagens com RabbitMQ.

## Arquitetura prevista
- RabbitMQ
- Producer
- Sentiment Consumer
- Support Consumer

## Iniciar o ambiente
```bash
docker compose up -d
```

Interface do RabbitMQ: http://localhost:15672

Para acompanhar as mensagens publicadas pelo Producer:
```bash
docker compose logs -f producer
```

Para acompanhar a classificação de avaliações:
```bash
docker compose logs -f sentiment-consumer
```

Para acompanhar a classificação de solicitações de suporte:
```bash
docker compose logs -f support-consumer
```

## Inteligência Artificial
Os dois consumidores treinam modelos locais K-Nearest Neighbors (KNN) do Smile na inicialização. Os textos são normalizados e convertidos em vetores binários usando um vocabulário local fixo. Sentiment classifica avaliações como `POSITIVA`, `NEGATIVA` ou `NEUTRA`; Support classifica solicitações como `PROBLEMA_ACESSO`, `PROBLEMA_DESEMPENHO` ou `PROBLEMA_CONEXAO`. Nenhuma API externa de IA é utilizada. Os modelos podem ser verificados dentro de cada container com `docker compose exec sentiment-consumer java -jar app.jar --verify-model` e `docker compose exec support-consumer java -jar app.jar --verify-model`.

## Dead Letter Queue (DLQ)
Mensagens que falharem no processamento são encaminhadas automaticamente para a DLQ, sem requeue.

- Exchange: `dead.letter.exchange`
- Fila: `queue.dlq`
- Routing key: `dead.letter`

Para testar, abra `http://localhost:15672`, entre em **Exchanges** > `messages.exchange` > **Publish message** e publique um JSON como:

```json
{"id":"teste-dlq","timestamp":"2026-10-13T16:00:00","type":"review.product","content":"FORCE_ERROR"}
```

Use `review.product` como routing key (ou `support.request` para testar o Support Consumer). Confira a mensagem em **Queues and Streams** > `queue.dlq`, ou execute:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
```
