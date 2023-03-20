package com.example.demorabbitmq.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClientRpc {
    private static final String QUEUE_NAME = "RPC_QUEUE";
    private static final String REPLY_QUEUE = "REPLY_QUEUE";

    @SneakyThrows
    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try (Connection connection = connectionFactory.newConnection();) {
            Channel channel = connection.createChannel();
            for (int i = 0; i < 5; i++) {
                String num = Integer.valueOf(i).toString();
                String corrId = UUID.randomUUID().toString();
                System.out.println("[.] Requesting fib :" + i  + " CorrId: " + corrId);
                String res = getValue(channel, num, corrId);
                System.out.println("[.] GOT:" + res );

            }

        }

    }

    private static String getValue(Channel channel, String num, String corrId) throws IOException, InterruptedException, ExecutionException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .build();
        channel.basicPublish("", QUEUE_NAME, properties, num.getBytes(StandardCharsets.UTF_8));
        CompletableFuture<String> completableFuture = new CompletableFuture<>();

        String tag = channel.basicConsume(REPLY_QUEUE, true, (consumerTag, message) -> {
            if (message.getProperties().getCorrelationId().equals(corrId)){
                completableFuture.complete(new String(message.getBody(), StandardCharsets.UTF_8));
            }
        }, consumerTag -> {});
        String res = completableFuture.get();
        channel.basicCancel(tag);
        return res;
    }
}
