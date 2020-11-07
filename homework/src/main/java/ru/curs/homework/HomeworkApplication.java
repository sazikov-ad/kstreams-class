package ru.curs.homework;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class HomeworkApplication {

  public static void main(String[] args) {
    new SpringApplicationBuilder(HomeworkApplication.class).headless(false).run(args);
  }

}
