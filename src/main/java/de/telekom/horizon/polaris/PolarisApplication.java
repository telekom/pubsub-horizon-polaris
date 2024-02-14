package de.telekom.horizon.polaris;

import de.telekom.eni.pandora.horizon.mongo.config.MongoProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({MongoProperties.class})
public class PolarisApplication {

	public static void main(String[] args) {
		SpringApplication.run(PolarisApplication.class, args);
	}

}
