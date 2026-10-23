package com.anhtran.accountservice.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.anhtran.accountservice.model.AccountDTO;
import com.anhtran.accountservice.service.AccountService;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
@Slf4j
public class AccountController {
    private final AccountService accountService;

    @GetMapping(value = "/checkBalance/{id}")
    public Mono<AccountDTO> checkBalance(@PathVariable String id){
        return accountService.checkBalance(id);
    }
}
    

