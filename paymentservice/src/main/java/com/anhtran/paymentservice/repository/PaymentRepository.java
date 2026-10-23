package com.anhtran.paymentservice.repository;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import com.anhtran.paymentservice.data.Payment;
import reactor.core.publisher.Flux;

public interface PaymentRepository extends ReactiveCrudRepository<Payment,Long> {
    @Query("SELECT * FROM payment WHERE account_id = :id")
    Flux<Payment> findByAccountId(String id);
}
