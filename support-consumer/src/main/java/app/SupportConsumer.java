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

public class SupportConsumer {
    private static final String EXCHANGE_NAME = "messages.exchange";
    private static final String DEAD_LETTER_EXCHANGE_NAME = "dead.letter.exchange";
    private static final String DEAD_LETTER_QUEUE_NAME = "queue.dlq";
    private static final String DEAD_LETTER_ROUTING_KEY = "dead.letter";
    private static final String QUEUE_NAME = "queue.support";
    private static final String ROUTING_KEY = "support.request";
    private static final long PROCESSING_DELAY_MILLIS = 1_000;
    private static final String[] CLASSES = {
            "PROBLEMA_ACESSO", "PROBLEMA_DESEMPENHO", "PROBLEMA_CONEXAO"
    };
    private static final List<String> VOCABULARY = List.of(
            "consigo", "acessar", "conta", "senha", "funciona", "login",
            "esqueci", "acesso", "bloqueado", "sistema", "autenticacao",
            "problema", "credenciais", "entrar", "erro", "aplicacao",
            "lento", "travando", "demora", "responder", "programa",
            "lentidao", "fica", "congelando", "processamento", "demorado",
            "perdeu", "desempenho", "pagina", "carregar", "trava", "durante",
            "uso", "conexao", "cai", "constantemente", "conectar", "servidor",
            "internet", "desconectando", "sem", "rede", "nao", "offline",
            "perdida", "toda", "hora"
    );
    private static final List<TrainingExample> TRAINING_DATA = List.of(
            new TrainingExample("Não consigo acessar minha conta", 0),
            new TrainingExample("Minha senha não funciona", 0),
            new TrainingExample("Não consigo fazer login", 0),
            new TrainingExample("Esqueci minha senha", 0),
            new TrainingExample("O acesso foi bloqueado", 0),
            new TrainingExample("Não consigo entrar no sistema", 0),
            new TrainingExample("Minha conta está bloqueada", 0),
            new TrainingExample("Erro de autenticação", 0),
            new TrainingExample("Problema com minhas credenciais", 0),
            new TrainingExample("O login não está funcionando", 0),
            new TrainingExample("O sistema está muito lento", 1),
            new TrainingExample("A aplicação está travando", 1),
            new TrainingExample("O sistema demora para responder", 1),
            new TrainingExample("O programa apresenta lentidão", 1),
            new TrainingExample("A aplicação fica congelando", 1),
            new TrainingExample("O processamento está muito demorado", 1),
            new TrainingExample("O sistema perdeu desempenho", 1),
            new TrainingExample("A página demora para carregar", 1),
            new TrainingExample("O programa está muito lento", 1),
            new TrainingExample("O sistema trava durante o uso", 1),
            new TrainingExample("Minha conexão cai constantemente", 2),
            new TrainingExample("Não consigo conectar ao servidor", 2),
            new TrainingExample("A internet está desconectando", 2),
            new TrainingExample("Estou sem conexão", 2),
            new TrainingExample("A rede não funciona", 2),
            new TrainingExample("O servidor está offline", 2),
            new TrainingExample("A conexão com o sistema foi perdida", 2),
            new TrainingExample("Não consigo conectar pela rede", 2),
            new TrainingExample("A internet cai toda hora", 2),
            new TrainingExample("Erro de conexão com o servidor", 2)
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

                    String category = classifySupportRequest(message.content());
                    System.out.println("[SUPPORT] ID: " + message.id());
                    System.out.println("[SUPPORT][SMILE]");
                    System.out.println("Texto: " + message.content());
                    System.out.println("Classificação: " + category);
                    Thread.sleep(PROCESSING_DELAY_MILLIS);
                    channel.basicAck(deliveryTag, false);
                } catch (Exception exception) {
                    if (exception instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    System.err.println("[SUPPORT] ERRO ao processar mensagem: "
                            + exception.getMessage());
                    try {
                        channel.basicNack(deliveryTag, false, false);
                        System.out.println("[SUPPORT] Mensagem enviada para DLQ");
                    } catch (IOException nackException) {
                        System.err.println("[SUPPORT] Erro ao rejeitar mensagem: "
                                + nackException.getMessage());
                    }
                }
            };

            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
            System.out.println("[SUPPORT] Consumindo mensagens da fila " + QUEUE_NAME);
            new CountDownLatch(1).await();
        } catch (Exception exception) {
            System.err.println("[SUPPORT] Falha ao iniciar consumidor: "
                    + exception.getMessage());
            throw new RuntimeException(exception);
        }
    }

    private static String classifySupportRequest(String text) {
        double[] vector = vectorize(text);
        if (Arrays.stream(vector).allMatch(value -> value == 0.0)) {
            // Fallback técnico para textos fora do vocabulário; o fluxo normal usa o modelo Smile.
            return "PROBLEMA_ACESSO";
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
        System.out.println("[SUPPORT][SMILE] Modelo KNN treinado com "
                + TRAINING_DATA.size() + " exemplos.");
        return model;
    }

    private static void verifyModel() {
        verifyExample("Não consigo acessar minha conta", "PROBLEMA_ACESSO");
        verifyExample("O sistema está muito lento", "PROBLEMA_DESEMPENHO");
        verifyExample("Minha conexão cai constantemente", "PROBLEMA_CONEXAO");
    }

    private static void verifyExample(String text, String expected) {
        String actual = classifySupportRequest(text);
        System.out.println("[SUPPORT][SMILE][TEST] " + text + " -> " + actual);
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
