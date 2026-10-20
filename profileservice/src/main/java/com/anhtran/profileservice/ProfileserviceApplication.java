package com.anhtran.profileservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableR2dbcRepositories
@EnableDiscoveryClient
@ComponentScan({"com.anhtran.profileservice", "com.anhtran.commonservice"})
public class ProfileserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProfileserviceApplication.class, args);
	}

}
