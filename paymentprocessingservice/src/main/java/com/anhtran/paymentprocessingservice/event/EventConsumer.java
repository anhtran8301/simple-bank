package com.anhtran.paymentprocessingservice.event;

import java.util.Collections;

import org.springframework.stereotype.Service;
import com.anhtran.commonservice.utils.Constant;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

@Service
@Slf4j
public class EventConsumer {
    public EventConsumer(ReceiverOptions<String,String> receiverOptions){
        KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(Constant.PAYMENT_CREATED_TOPIC)))
                .receive()
                .doOnError(e -> log.error("Kafka consumer error: ", e))
                .subscribe(this::paymentCreated);
    }

    public void paymentCreated(ReceiverRecord<String,String> receiverRecord){
        log.info("Processing payment");
        
    }
}
