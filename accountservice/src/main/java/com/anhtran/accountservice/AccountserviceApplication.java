package com.anhtran.accountservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
@SpringBootApplication
@EnableR2dbcRepositories
@EnableDiscoveryClient
@ComponentScan({"com.anhtran.accountservice", "com.anhtran.commonservice"})
public class AccountserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AccountserviceApplication.class, args);
	}

}
