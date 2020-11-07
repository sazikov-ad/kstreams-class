package ru.curs.homework.configuration;

import static ru.curs.counting.model.TopicNames.BET_TOPIC;
import static ru.curs.counting.model.TopicNames.EVENT_SCORE_TOPIC;
import static ru.curs.counting.model.TopicNames.FRAUD_TOPIC;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

@Configurable
public class KafkaConfiguration {

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration getStreamsConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "homework-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
    props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
    return new KafkaStreamsConfiguration(props);
  }

}
