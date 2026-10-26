package com.anhtran.paymentprocessingservice.event;

import java.security.SecureRandom;
import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.anhtran.commonservice.model.PaymentDTO;
import com.anhtran.commonservice.utils.Constant;
import com.google.gson.Gson;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

@Service
@Slf4j
public class EventConsumer {
    Gson gson = new Gson();

    SecureRandom random = new SecureRandom();

    @Autowired
    EventProducer eventProducer;

    public EventConsumer(ReceiverOptions<String,String> receiverOptions){
        KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(Constant.PAYMENT_CREATED_TOPIC)))
                .receive()
                .doOnError(e -> log.error("Kafka consumer error: ", e))
                .subscribe(this::paymentCreated);
    }

    @SneakyThrows 
    public void paymentCreated(ReceiverRecord<String,String> receiverRecord){
        log.info("Processing payment");
        PaymentDTO paymentDTO = gson.fromJson(receiverRecord.value(), PaymentDTO.class);

        // Giả sử random trạng thái xử lý là thành công hoặc thất bại
        String[] randomStatus = {Constant.STATUS_PAYMENT_REJECTED, Constant.STATUS_PAYMENT_SUCCESSFUL};
        int index = random.nextInt(randomStatus.length);
        paymentDTO.setStatus(randomStatus[index]);

        log.info("Sending payment completed event");
        Thread.sleep(3000); // Giả lập thời gian xử lý

        eventProducer.send(Constant.PAYMENT_COMPLETED_TOPIC, gson.toJson(paymentDTO)).subscribe();
    }
}
