package com.gayanath.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    Logger logger = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("quickstart-events", message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("---------------------------------------------------------------");
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
                logger.info("---------------------------------------------------------------");
            } else {
                logger.info("---------------------------------------------------------------");
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
                logger.info("---------------------------------------------------------------");
            }
        });
    }


}
