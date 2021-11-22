package com.example.demobatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DemoBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoBatchApplication.class, args);
	}

}
