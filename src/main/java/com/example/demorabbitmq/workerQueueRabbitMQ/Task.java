package com.example.demorabbitmq.workerQueueRabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class Task {
    //    private final static String QUEUE_NAME;
    private final static String QUEUE_NAME = "DURABLE_QUEUE";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        String[] messages = {"First message...................", "Second message..", "Third message..", "Fourth message..", "Fifth message..", "Sixth message.."};
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();) {

            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            for (String message : messages) {
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                System.out.println(" [x] Sent " + message);

            }


        }
    }
}
