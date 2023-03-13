package com.example.demorabbitmq.routingPublishSubscribe;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.nio.charset.StandardCharsets;

public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "DIRECT_LOGS";
    private final static String[] messages = {"First message.........", "Second message.........", "Third message.........", "Fourth message.........", "Fifth message.........", "Sixth message........."};

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            for (int i = 0; i < messages.length; i++) {
                String msg = messages[i];
                if ((i + 1) % 2 == 0) {
                    channel.basicPublish(
                            EXCHANGE_NAME,
                            "even",
                            MessageProperties.PERSISTENT_TEXT_PLAIN,
                            msg.getBytes(StandardCharsets.UTF_8));
                } else {
                    channel.basicPublish(
                            EXCHANGE_NAME,
                            "odd",
                            MessageProperties.PERSISTENT_TEXT_PLAIN,
                            msg.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }
}
