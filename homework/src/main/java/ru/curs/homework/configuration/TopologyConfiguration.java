package ru.curs.homework.configuration;

import static ru.curs.counting.model.TopicNames.BET_TOPIC;
import static ru.curs.counting.model.TopicNames.EVENT_SCORE_TOPIC;
import static ru.curs.counting.model.TopicNames.TEAM_BET_TOPIC;
import static ru.curs.counting.model.TopicNames.USER_BET_TOPIC;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Outcome;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {


  @Bean
  public Topology createTopology(StreamsBuilder streamsBuilder) {
    KStream<String, Bet> betsInput = streamsBuilder.stream(
        BET_TOPIC,
        Consumed.with(Serdes.String(), new JsonSerde<>(Bet.class))
            .withTimestampExtractor((record, previousTimestamp) ->
                ((Bet) record.value()).getTimestamp()));
    KStream<String, EventScore> scoresInput = streamsBuilder.stream(
        EVENT_SCORE_TOPIC,
        Consumed.with(Serdes.String(), new JsonSerde<>(EventScore.class))
            .withTimestampExtractor((record, previousTimestamp) ->
                ((EventScore) record.value()).getTimestamp()));

    KTable<String, Long> user_bets = betsInput
        .map((k, v) -> KeyValue.pair(v.getBettor(), v.getAmount()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())).reduce(Long::sum);

    KTable<String, Long> team_bets = betsInput
        .filter((k, v) -> v.getOutcome() != Outcome.D)
        .map((k, v) -> KeyValue.pair(
            v.getMatch().split("-")[v.getOutcome() == Outcome.H ? 0 : 1],
            v.getAmount()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())).reduce(Long::sum);

    user_bets.toStream()
        .to(USER_BET_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    team_bets.toStream().to(TEAM_BET_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    Topology topology = streamsBuilder.build();
    System.out.println("========================================");
    System.out.println(topology.describe());
    System.out.println("========================================");
    // https://zz85.github.io/kafka-streams-viz/
    return topology;
  }

}
