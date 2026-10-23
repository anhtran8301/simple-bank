package com.anhtran.paymentservice.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.anhtran.paymentservice.model.PaymentDTO;
import com.anhtran.paymentservice.service.PaymentService;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @GetMapping(value = "/{id}")
    public ResponseEntity<Flux<PaymentDTO>> getAllPayment(@PathVariable String id){
        return ResponseEntity.ok(paymentService.getAllPayment(id));
    }

    @PostMapping(value = "/payment")
    public ResponseEntity<Mono<PaymentDTO>> makePayment(@RequestBody PaymentDTO paymentDTO){
        return ResponseEntity.status(HttpStatus.CREATED).body(paymentService.makePayment(paymentDTO));
    }
}
