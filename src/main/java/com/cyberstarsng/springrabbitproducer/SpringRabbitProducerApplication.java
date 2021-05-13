package com.cyberstarsng.springrabbitproducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@SpringBootApplication
public class SpringRabbitProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringRabbitProducerApplication.class, args);
    }

}

@Component
@RequiredArgsConstructor
@Slf4j
class Producer {

    private final AmqpTemplate template;

    @Value("${spring.rabbitmq.exchange}")
    private String exchange;

    @Value("${spring.rabbitmq.routing-key}")
    private String routingKey;

    public void produceMessage(String records) {
        log.info("about to send message ...");
        template.convertAndSend(exchange, routingKey, records);
        log.info("message sent successfully... {}", records);
    }


}

@RestController
@RequiredArgsConstructor
class RecordResource {

    private final Producer producer;
    private final ObjectMapper mapper;

    @PostMapping("sendRecord")
    public ResponseEntity<Records> sendRecord(@RequestBody Records records) throws JsonProcessingException {
        String data = mapper.writeValueAsString(records);
        producer.produceMessage(data);
        return ResponseEntity.ok(records);
    }

}

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
class Records implements Serializable {

    private Long id;
    private String name;
    private String gender;
    private String email;
}

@Service
@EnableScheduling
@RequiredArgsConstructor
class CustomScheduler {

    private final Producer producer;
    private final ObjectMapper mapper;

    @Scheduled(fixedDelay = 60000)
    public void processRecords() throws JsonProcessingException {
        Faker faker = new Faker();
        List<String> genders = Arrays.asList("Male", "Female");
        Random random = new Random();
        String sex = genders.get(random.nextInt(genders.size()));
        Records record = Records.builder()
                .id(RandomUtils.nextLong())
                .name(faker.name().fullName())
                .email(faker.name().username().concat("@email.com"))
                .gender(sex)
                .build();
        String data = mapper.writeValueAsString(record);
        producer.produceMessage(data);
    }

}
