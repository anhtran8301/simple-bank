package com.anhtran.paymentservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.anhtran.commonservice.common.CommonException;
import com.anhtran.commonservice.model.AccountDTO;
import com.anhtran.commonservice.utils.Constant;
import com.anhtran.paymentservice.model.PaymentDTO;
import com.anhtran.paymentservice.repository.PaymentRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final WebClient webClientAccount;

    public Flux<PaymentDTO> getAllPayment(String id) {
        return paymentRepository.findByAccountId(id)
                .map(PaymentDTO::entityToDto)
                .switchIfEmpty(Mono.error(new CommonException("P02", "Account don't have payment", HttpStatus.NOT_FOUND)));
    }

    public Mono<PaymentDTO> makePayment(PaymentDTO paymentDTO) {
        return webClientAccount.get()
                .uri("/checkBalance/{accountId}", paymentDTO.getAccountId())
                .retrieve()
                .bodyToMono(AccountDTO.class)
                .flatMap(accountDTO -> {
                    if (paymentDTO.getAmount() > accountDTO.getBalance()) {
                        throw new CommonException("P01", "Balance not enough", HttpStatus.BAD_REQUEST);
                    } 

                    paymentDTO.setStatus(Constant.STATUS_PAYMENT_CREATING);

                    return createNewPayment(paymentDTO);
                });
    }

    public Mono<PaymentDTO> createNewPayment(PaymentDTO paymentDTO) {
        return Mono.just(paymentDTO)
                .map(PaymentDTO::dtoToEntity)
                .flatMap(payment -> paymentRepository.save(payment))
                .map(PaymentDTO::entityToDto)
                .doOnError(throwable -> log.error(throwable.getMessage()));
    }
}
