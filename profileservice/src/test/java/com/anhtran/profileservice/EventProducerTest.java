package com.anhtran.profileservice;

import com.anhtran.profileservice.event.EventProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventProducerTest {

    @Mock
    private KafkaSender<String, String> sender;

    @InjectMocks
    private EventProducer eventProducer;

    @Captor
    private ArgumentCaptor<Publisher<? extends SenderRecord<String, String, String>>> recordsCaptor;

    @Test
    void sendPublishesRecordAndReturnsOk() {
        String topic = "profile-onboarding";
        String message = "{\"email\":\"test@gmail.com\"}";
        when(sender.send(any())).thenReturn(Flux.empty());

        StepVerifier.create(eventProducer.send(topic, message))
                .expectNext("OK")
                .verifyComplete();

        verify(sender).send(recordsCaptor.capture());
        StepVerifier.create(Flux.from(recordsCaptor.getValue()))
                .assertNext(record -> {
                    assertEquals(topic, record.topic());
                    assertEquals(message, record.value());
                    assertEquals(message, record.correlationMetadata());
                })
                .verifyComplete();
    }

    @Test
    void sendIgnoresSenderResultsAndReturnsOk() {
        @SuppressWarnings("unchecked")
        SenderResult<String> result = mock(SenderResult.class);
        when(sender.<String>send(any())).thenReturn(Flux.just(result));

        StepVerifier.create(eventProducer.send("profile-onboarding", "message"))
                .expectNext("OK")
                .verifyComplete();

        verify(sender).send(any());
    }

    @Test
    void sendPropagatesSenderError() {
        RuntimeException error = new RuntimeException("Kafka unavailable");
        when(sender.send(any())).thenReturn(Flux.error(error));

        StepVerifier.create(eventProducer.send("profile-onboarding", "message"))
                .expectErrorMatches(actual -> actual == error)
                .verify();

        verify(sender).send(any());
    }
}
