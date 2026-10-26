package com.anhtran.accountservice.event;

import com.google.gson.Gson;
import com.anhtran.accountservice.service.AccountService;
import com.anhtran.accountservice.model.AccountDTO;
import com.anhtran.commonservice.model.PaymentDTO;
import com.anhtran.commonservice.model.ProfileDTO;
import com.anhtran.commonservice.utils.Constant;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;

@Service
@Slf4j
public class EventConsumer {
    Gson gson = new Gson(); // convert Json -> DTO

    @Autowired
    AccountService accountService;

    @Autowired
    EventProducer eventProducer;

    public EventConsumer(ReceiverOptions<String,String> receiverOptions){
        KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(Constant.PROFILE_ONBOARDING_TOPIC)))
                .receive()
                .doOnError(e -> log.error("Kafka consumer error: ", e))
                .subscribe(this::profileOnboarding);

        KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(Constant.PAYMENT_REQUEST_TOPIC)))
                .receive()
                .doOnError(e -> log.error("Kafka consumer error: ", e))
                .subscribe(this::paymentRequest);
    }

    public void profileOnboarding(ReceiverRecord<String,String> receiverRecord){
        log.info("Profile Onboarding event");
        ProfileDTO dto = gson.fromJson(receiverRecord.value(),ProfileDTO.class);
        AccountDTO accountDTO = new AccountDTO();
        accountDTO.setId(String.valueOf(dto.getId()));
        accountDTO.setEmail(dto.getEmail());
        accountDTO.setReserved(0);
        accountDTO.setBalance(dto.getInitialBalance());
        accountDTO.setCurrency("USD");
        accountService.createNewAccount(accountDTO).subscribe(res ->{
            dto.setStatus(Constant.STATUS_PROFILE_ACTIVE);
            eventProducer.send(Constant.PROFILE_ONBOARDED_TOPIC,gson.toJson(dto)).subscribe();
        });
    }

    public void paymentRequest(ReceiverRecord<String,String> receiverRecord){
        PaymentDTO paymentDTO = gson.fromJson(receiverRecord.value(), PaymentDTO.class);
        accountService.bookAmount(paymentDTO.getAmount(), paymentDTO.getAccountId()).subscribe();
    }
}
