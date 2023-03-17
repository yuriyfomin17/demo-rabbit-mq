package com.example.demorabbitmq.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.nio.charset.StandardCharsets;

public class EmitLogTopic {
    private static final String EXCHANGE_NAME = "TOPIC_EXCHANGE";
    private static final String[] animals = {"DOG", "CAT"};
    private static final String[] colors = {"BROWN", "WHITE"};
    private static final String[] characters = {"HARD_WORKING", "LAZY"};

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();) {
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            String[] routingKeyWords = getRoutingKeys();
            for (String routingKeyWord : routingKeyWords) {
                String message = String.format("message.., routingKey:%s", routingKeyWord);
                channel.basicPublish(
                        EXCHANGE_NAME,
                        routingKeyWord,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes(StandardCharsets.UTF_8)
                );

            }
        }
    }

    private static String[] getRoutingKeys() {
        String[] keys = new String[characters.length * colors.length * animals.length];
        int idx = 0;
        for (String animal : animals) {
            for (String color : colors) {
                for (String character : characters) {
                    String routingKey = String.format("%s.%s.%s", animal, color, character);
                    keys[idx] = routingKey;
                    idx += 1;
                }
            }
        }
        return keys;
    }

}
