package com.anhtran.profileservice.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventProducer {
    private final KafkaSender<String, String> sender;

    public Mono<String> send(String topic, String message) {
        return sender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(topic, message), message)))
                .doOnError(err -> log.error("Failed to send message: {}", err.getMessage()))
                .then()
                .thenReturn("OK");
    }
}