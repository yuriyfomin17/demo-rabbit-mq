package com.example.demorabbitmq.rpc;

import com.rabbitmq.client.*;
import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;

public class ServerRpc {
    private static final String QUEUE_NAME = "RPC_QUEUE";
    private static final String REPLY_QUEUE = "REPLY_QUEUE";

    @SneakyThrows
    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
//        channel.basicQos(1);
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueDeclare(REPLY_QUEUE, false, false, false, null);
        DeliverCallback deliverCallback = (tag, message) -> {
            String corrId = message.getProperties().getCorrelationId();
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(corrId)
                    .build();
            String msg = new String(message.getBody(), StandardCharsets.UTF_8);
            System.out.println("Processing Correlation Id:" + corrId);
            String res = fibonacci(Integer.parseInt(msg)).toString();
            channel.basicPublish("", REPLY_QUEUE, replyProps, res.getBytes(StandardCharsets.UTF_8));
            channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        };
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, (consumerTag -> {}));
    }

    @SneakyThrows
    static Integer fibonacci(int n) {
        Thread.sleep(1000);
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fibonacci(n - 1) + fibonacci(n - 2);
    }
}
