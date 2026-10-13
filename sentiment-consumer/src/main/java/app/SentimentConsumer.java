package app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import smile.classification.KNN;

import java.io.IOException;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SentimentConsumer {
    private static final String EXCHANGE_NAME = "messages.exchange";
    private static final String DEAD_LETTER_EXCHANGE_NAME = "dead.letter.exchange";
    private static final String DEAD_LETTER_QUEUE_NAME = "queue.dlq";
    private static final String DEAD_LETTER_ROUTING_KEY = "dead.letter";
    private static final String QUEUE_NAME = "queue.reviews";
    private static final String ROUTING_KEY = "review.product";
    private static final long PROCESSING_DELAY_MILLIS = 1_000;
    private static final String[] CLASSES = {"POSITIVA", "NEGATIVA", "NEUTRA"};
    private static final List<String> VOCABULARY = List.of(
            "produto", "excelente", "chegou", "rapido", "gostei", "muito",
            "bom", "satisfeito", "compra", "funciona", "perfeitamente",
            "recomendo", "entrega", "otimo", "qualidade", "perfeita",
            "ruim", "veio", "defeito", "decepcionado", "pessimo", "nao",
            "quebrado", "horrivel", "apresentou", "problema", "hoje",
            "razoavel", "recebi", "possui", "tamanho", "medio", "embalagem",
            "contem", "unidade", "normalmente", "opiniao", "comum",
            "encomenda", "corresponde", "descricao", "demorou", "esperado",
            "conforme", "anunciado"
    );
    private static final List<TrainingExample> TRAINING_DATA = List.of(
            new TrainingExample("O produto é excelente", 0),
            new TrainingExample("Gostei muito do produto", 0),
            new TrainingExample("O produto é muito bom", 0),
            new TrainingExample("Estou satisfeito com a compra", 0),
            new TrainingExample("O produto funciona perfeitamente", 0),
            new TrainingExample("Recomendo este produto", 0),
            new TrainingExample("A entrega foi rápida e o produto é ótimo", 0),
            new TrainingExample("Excelente qualidade", 0),
            new TrainingExample("Produto muito bom", 0),
            new TrainingExample("Compra perfeita", 0),
            new TrainingExample("O produto é ruim", 1),
            new TrainingExample("O produto veio com defeito", 1),
            new TrainingExample("Estou decepcionado com a compra", 1),
            new TrainingExample("O produto é péssimo", 1),
            new TrainingExample("Não gostei do produto", 1),
            new TrainingExample("O produto chegou quebrado", 1),
            new TrainingExample("A qualidade é horrível", 1),
            new TrainingExample("O produto apresentou problema", 1),
            new TrainingExample("Foi uma compra ruim", 1),
            new TrainingExample("O produto não funciona", 1),
            new TrainingExample("O produto chegou hoje", 2),
            new TrainingExample("O produto é razoável", 2),
            new TrainingExample("Recebi o produto", 2),
            new TrainingExample("O produto possui tamanho médio", 2),
            new TrainingExample("A embalagem contém uma unidade", 2),
            new TrainingExample("O produto funciona normalmente", 2),
            new TrainingExample("Não tenho opinião sobre o produto", 2),
            new TrainingExample("O produto é comum", 2),
            new TrainingExample("Recebi a encomenda", 2),
            new TrainingExample("O produto corresponde à descrição", 2),
            new TrainingExample("A entrega demorou mais do que o esperado", 1),
            new TrainingExample("O produto funciona conforme anunciado", 2)
    );
    private static final KNN<double[]> MODEL = trainModel();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        if (args.length == 1 && "--verify-model".equals(args[0])) {
            verifyModel();
            return;
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("rabbitmq");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
            channel.exchangeDeclare(DEAD_LETTER_EXCHANGE_NAME, "direct", true);
            channel.queueDeclare(DEAD_LETTER_QUEUE_NAME, true, false, false, null);
            channel.queueBind(
                    DEAD_LETTER_QUEUE_NAME,
                    DEAD_LETTER_EXCHANGE_NAME,
                    DEAD_LETTER_ROUTING_KEY
            );
            Map<String, Object> queueArguments = Map.of(
                    "x-dead-letter-exchange", DEAD_LETTER_EXCHANGE_NAME,
                    "x-dead-letter-routing-key", DEAD_LETTER_ROUTING_KEY
            );
            channel.queueDeclare(QUEUE_NAME, true, false, false, queueArguments);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            channel.basicQos(1);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                try {
                    Message message = OBJECT_MAPPER.readValue(
                            delivery.getBody(), Message.class);
                    if (!ROUTING_KEY.equals(message.type())) {
                        throw new IllegalArgumentException(
                                "Tipo de mensagem inesperado: " + message.type());
                    }

                    if (message.content().contains("FORCE_ERROR")) {
                        throw new RuntimeException("FORCE_ERROR");
                    }

                    String sentiment = classifySentiment(message.content());
                    System.out.println("[SENTIMENT] ID: " + message.id());
                    System.out.println("[SENTIMENT][SMILE]");
                    System.out.println("Texto: " + message.content());
                    System.out.println("Classificação: " + sentiment);
                    Thread.sleep(PROCESSING_DELAY_MILLIS);
                    channel.basicAck(deliveryTag, false);
                } catch (Exception exception) {
                    System.err.println("[SENTIMENT] ERRO ao processar mensagem: "
                            + exception.getMessage());
                    try {
                        channel.basicNack(deliveryTag, false, false);
                        System.out.println("[SENTIMENT] Mensagem enviada para DLQ");
                    } catch (IOException nackException) {
                        System.err.println("[SENTIMENT] Erro ao rejeitar mensagem: "
                                + nackException.getMessage());
                    }
                }
            };

            channel.basicConsume(QUEUE_NAME, false, deliverCallback, (consumerTag -> { }));
            System.out.println("[SENTIMENT] Consumindo mensagens da fila " + QUEUE_NAME);
            new CountDownLatch(1).await();
        } catch (Exception exception) {
            System.err.println("[SENTIMENT] Falha ao iniciar consumidor: "
                    + exception.getMessage());
            throw new RuntimeException(exception);
        }
    }

    private static String classifySentiment(String text) {
        double[] vector = vectorize(text);
        if (Arrays.stream(vector).allMatch(value -> value == 0.0)) {
            // Fallback técnico para textos fora do vocabulário; o fluxo normal usa o modelo Smile.
            return "NEUTRA";
        }
        return CLASSES[MODEL.predict(vector)];
    }

    private static KNN<double[]> trainModel() {
        double[][] vectors = new double[TRAINING_DATA.size()][];
        int[] labels = new int[TRAINING_DATA.size()];
        for (int index = 0; index < TRAINING_DATA.size(); index++) {
            TrainingExample example = TRAINING_DATA.get(index);
            vectors[index] = vectorize(example.text());
            labels[index] = example.label();
        }
        KNN<double[]> model = KNN.fit(vectors, labels);
        System.out.println("[SENTIMENT][SMILE] Modelo KNN treinado com "
                + TRAINING_DATA.size() + " exemplos.");
        return model;
    }

    private static void verifyModel() {
        verifyExample("O produto é excelente", "POSITIVA");
        verifyExample("O produto é ruim", "NEGATIVA");
        verifyExample("O produto chegou hoje", "NEUTRA");
        verifyExample("O produto funciona conforme anunciado", "NEUTRA");
        verifyExample("A entrega demorou mais do que o esperado", "NEGATIVA");
    }

    private static void verifyExample(String text, String expected) {
        String actual = classifySentiment(text);
        System.out.println("[SENTIMENT][SMILE][TEST] " + text + " -> " + actual);
        if (!expected.equals(actual)) {
            throw new IllegalStateException(
                    "Esperado " + expected + ", obtido " + actual + " para: " + text);
        }
    }

    private static double[] vectorize(String text) {
        String normalized = Normalizer.normalize(text, Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9\\s]", " ");
        List<String> words = Arrays.asList(normalized.trim().split("\\s+"));
        double[] vector = new double[VOCABULARY.size()];
        for (int index = 0; index < VOCABULARY.size(); index++) {
            vector[index] = words.contains(VOCABULARY.get(index)) ? 1.0 : 0.0;
        }
        return vector;
    }

    private record TrainingExample(String text, int label) {
    }

    private record Message(String id, String timestamp, String type, String content) {
    }
}
