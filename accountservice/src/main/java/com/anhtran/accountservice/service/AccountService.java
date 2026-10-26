package com.anhtran.accountservice.service;

import com.anhtran.accountservice.model.AccountDTO;
import com.anhtran.accountservice.repository.AccountRepository;
import com.anhtran.commonservice.common.CommonException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {
    private final AccountRepository accountRepository;

    public Mono<AccountDTO> createNewAccount(AccountDTO accountDTO){
        return Mono.just(accountDTO)
                .map(AccountDTO::dtoToEntity)
                .flatMap(account -> accountRepository.save(account))
                .map(AccountDTO::entityToModel)
                .doOnError(throwable ->
                        log.error(throwable.getMessage())
                );
    }

    public Mono<AccountDTO> checkBalance(String id){
        return findById(id);
    }

    public Mono<Boolean> bookAmount(double amount, String accountId){
        return accountRepository.findById(accountId)
                .switchIfEmpty(
                        Mono.error(
                                new CommonException("A01", "Account not found", HttpStatus.NOT_FOUND))
                )
                .flatMap(account -> {
                    // Kiểm tra để đảm bảo rằng số dư không được nhỏ hơn số tiền đặt chỗ + số tiền đã được đặt chỗ trước đó
                    if (account.getBalance() < account.getReserved() + amount) {
                        return Mono.just(false);
                    }
                    
                    // Set giá trị đặt chỗ mới của account đó
                    account.setReserved(account.getReserved() + amount);

                    return accountRepository.save(account);
                })
                .flatMap(account -> Mono.just(true));
    }

    public Mono<AccountDTO> findById(String id){
        return accountRepository.findById(id)
                .map(AccountDTO::entityToModel)
                .switchIfEmpty(
                        Mono.error(
                                new CommonException("A01", "Account not found", HttpStatus.NOT_FOUND))
                );
    }
}
