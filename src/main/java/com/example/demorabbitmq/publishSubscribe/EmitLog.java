package com.example.demorabbitmq.publishSubscribe;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class EmitLog {
    private final static String EXCHANGE_NAME = "logs";

    private final static String[] messages = {"First message...................", "Second message..", "Third message..", "Fourth message..", "Fifth message..", "Sixth message.."};

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();) {
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            for (String message : messages) {
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent " + message);
            }


        }
    }
}
