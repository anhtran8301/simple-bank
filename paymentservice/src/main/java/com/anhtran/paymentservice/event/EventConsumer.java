package com.anhtran.paymentservice.event;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.anhtran.commonservice.utils.Constant;
import com.anhtran.paymentservice.model.PaymentDTO;
import com.anhtran.paymentservice.service.PaymentService;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

@Service
@Slf4j
public class EventConsumer {
    Gson gson = new Gson();

    @Autowired 
    PaymentService paymentService;

    public EventConsumer(ReceiverOptions<String,String> receiverOptions){
        KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(Constant.PAYMENT_CREATED_TOPIC)))
                .receive()
                .doOnError(e -> log.error("Kafka consumer error: ", e))
                .subscribe(this::paymentCreated);
        
        KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(Constant.PAYMENT_COMPLETED_TOPIC)))
                .receive()
                .doOnError(e -> log.error("Kafka consumer error: ", e))
                .subscribe(this::paymentComplete);
    }

    public void paymentCreated(ReceiverRecord<String,String> receiverRecord){
        log.info("Payment created event");
        PaymentDTO paymentDTO = gson.fromJson(receiverRecord.value(), PaymentDTO.class);
        paymentService.updateStatusPayment(paymentDTO).subscribe(result -> log.info("Update status " + result));
    }

    public void paymentComplete(ReceiverRecord<String,String> receiverRecord){
        log.info("Payment complete event");
        PaymentDTO paymentDTO = gson.fromJson(receiverRecord.value(), PaymentDTO.class);
        paymentService.updateStatusPayment(paymentDTO).subscribe(result -> log.info("End process payment " + result));
    }
}

