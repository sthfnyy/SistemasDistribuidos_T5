package app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MessageProducer {
    private static final String EXCHANGE_NAME = "messages.exchange";
    private static final String REVIEW_ROUTING_KEY = "review.product";
    private static final String SUPPORT_ROUTING_KEY = "support.request";
    private static final long MESSAGE_INTERVAL_MILLIS = 200;

    private static final List<String> REVIEWS = List.of(
            "O produto é excelente e chegou rápido.",
            "O produto veio com defeito.",
            "O produto é razoável.",
            "Gostei muito da compra.",
            "O produto não atendeu às expectativas.",
            "A qualidade é boa pelo preço.",
            "A entrega demorou mais do que o esperado.",
            "O produto funciona conforme anunciado."
    );

    private static final List<String> SUPPORT_REQUESTS = List.of(
            "Não consigo acessar minha conta.",
            "Esqueci minha senha.",
            "O sistema está muito lento.",
            "A aplicação está travando.",
            "Minha conexão cai constantemente.",
            "Não consigo conectar ao servidor."
    );

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("rabbitmq");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
            publishContinuously(channel);
        } catch (Exception exception) {
            System.err.println("[PRODUCER] Falha ao conectar ou publicar mensagens: "
                    + exception.getMessage());
            throw new RuntimeException(exception);
        }
    }

    private static void publishContinuously(Channel channel) throws Exception {
        while (!Thread.currentThread().isInterrupted()) {
            boolean isReview = ThreadLocalRandom.current().nextBoolean();
            String routingKey = isReview ? REVIEW_ROUTING_KEY : SUPPORT_ROUTING_KEY;
            List<String> contents = isReview ? REVIEWS : SUPPORT_REQUESTS;
            String content = contents.get(ThreadLocalRandom.current().nextInt(contents.size()));

            Message message = new Message(
                    UUID.randomUUID().toString(),
                    LocalDateTime.now().toString(),
                    routingKey,
                    content
            );
            String json = OBJECT_MAPPER.writeValueAsString(message);

            channel.basicPublish(
                    EXCHANGE_NAME,
                    routingKey,
                    null,
                    json.getBytes(StandardCharsets.UTF_8)
            );
            System.out.println("[PRODUCER] Routing Key: " + routingKey);
            System.out.println("[PRODUCER] Mensagem: " + json);

            try {
                Thread.sleep(MESSAGE_INTERVAL_MILLIS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private record Message(String id, String timestamp, String type, String content) {
    }
}
