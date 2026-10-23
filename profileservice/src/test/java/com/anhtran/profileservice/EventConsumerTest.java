package com.anhtran.profileservice;

import com.anhtran.commonservice.utils.Constant;
import com.anhtran.profileservice.event.EventConsumer;
import com.anhtran.profileservice.model.ProfileDTO;
import com.anhtran.profileservice.service.ProfileService;
import com.google.gson.JsonSyntaxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventConsumerTest {

    @Mock
    private ReceiverOptions<String, String> receiverOptions;

    @Mock
    private KafkaReceiver<String, String> receiver;

    @Mock
    private ReceiverRecord<String, String> receiverRecord;

    @Mock
    private ProfileService profileService;

    @Test
    void constructorSubscribesToProfileOnboardedTopic() {
        AtomicBoolean subscribed = new AtomicBoolean();
        when(receiverOptions.subscription(Collections.singleton(Constant.PROFILE_ONBOARDED_TOPIC)))
                .thenReturn(receiverOptions);
        when(receiver.receive()).thenReturn(Flux.defer(() -> {
            subscribed.set(true);
            return Flux.never();
        }));

        try (MockedStatic<KafkaReceiver> kafkaReceiver = mockStatic(KafkaReceiver.class)) {
            kafkaReceiver.when(() -> KafkaReceiver.create(receiverOptions)).thenReturn(receiver);

            new EventConsumer(receiverOptions);

            kafkaReceiver.verify(() -> KafkaReceiver.create(receiverOptions));
        }

        verify(receiverOptions).subscription(Collections.singleton(Constant.PROFILE_ONBOARDED_TOPIC));
        verify(receiver).receive();
        assertTrue(subscribed.get());
    }

    @Test
    void profileOnboardedUpdatesStatusAndSubscribesToServiceResult() {
        EventConsumer consumer = createConsumer(Flux.never());
        AtomicBoolean subscribed = new AtomicBoolean();
        when(receiverRecord.value()).thenReturn("{\"id\":1,\"email\":\"test@gmail.com\",\"status\":\"ACTIVE\"}");
        when(profileService.updateStatusProfile(any())).thenAnswer(invocation ->
                Mono.defer(() -> {
                    subscribed.set(true);
                    return Mono.just(invocation.getArgument(0));
                }));

        consumer.profileOnboarded(receiverRecord);

        ArgumentCaptor<ProfileDTO> dtoCaptor = ArgumentCaptor.forClass(ProfileDTO.class);
        verify(profileService).updateStatusProfile(dtoCaptor.capture());
        assertEquals(1, dtoCaptor.getValue().getId());
        assertEquals("test@gmail.com", dtoCaptor.getValue().getEmail());
        assertEquals(Constant.STATUS_PROFILE_ACTIVE, dtoCaptor.getValue().getStatus());
        assertTrue(subscribed.get());
    }

    @Test
    void profileOnboardedRejectsMalformedJson() {
        EventConsumer consumer = createConsumer(Flux.never());
        when(receiverRecord.value()).thenReturn("{bad json");

        assertThrows(JsonSyntaxException.class, () -> consumer.profileOnboarded(receiverRecord));

        verifyNoInteractions(profileService);
    }

    @Test
    void receivedKafkaRecordCallsProfileService() {
        Sinks.Many<ReceiverRecord<String, String>> records = Sinks.many().multicast().onBackpressureBuffer();
        createConsumer(records.asFlux());
        when(receiverRecord.value()).thenReturn("{\"email\":\"test@gmail.com\",\"status\":\"ACTIVE\"}");
        when(profileService.updateStatusProfile(any())).thenReturn(Mono.empty());

        assertEquals(Sinks.EmitResult.OK, records.tryEmitNext(receiverRecord));

        ArgumentCaptor<ProfileDTO> dtoCaptor = ArgumentCaptor.forClass(ProfileDTO.class);
        verify(profileService).updateStatusProfile(dtoCaptor.capture());
        assertEquals("test@gmail.com", dtoCaptor.getValue().getEmail());
        assertEquals(Constant.STATUS_PROFILE_ACTIVE, dtoCaptor.getValue().getStatus());
    }

    private EventConsumer createConsumer(Flux<ReceiverRecord<String, String>> records) {
        when(receiverOptions.subscription(Collections.singleton(Constant.PROFILE_ONBOARDED_TOPIC)))
                .thenReturn(receiverOptions);
        when(receiver.receive()).thenReturn(records);

        try (MockedStatic<KafkaReceiver> kafkaReceiver = mockStatic(KafkaReceiver.class)) {
            kafkaReceiver.when(() -> KafkaReceiver.create(receiverOptions)).thenReturn(receiver);
            EventConsumer consumer = new EventConsumer(receiverOptions);
            ReflectionTestUtils.setField(consumer, "profileService", profileService);
            return consumer;
        }
    }
}
