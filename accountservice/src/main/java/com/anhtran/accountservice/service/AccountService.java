package com.anhtran.accountservice.service;

import com.anhtran.accountservice.model.AccountDTO;
import com.anhtran.accountservice.repository.AccountRepository;
import com.anhtran.commonservice.common.CommonException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

    public Mono<AccountDTO> findById(String id){
        return accountRepository.findById(id)
                .map(AccountDTO::entityToModel)
                .switchIfEmpty(
                        Mono.error(new CommonException("A01", "Account not found", HttpStatus.NOT_FOUND)));
    }

}
