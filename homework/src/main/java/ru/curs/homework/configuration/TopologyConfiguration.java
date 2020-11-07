package ru.curs.homework.configuration;

import static ru.curs.counting.model.TopicNames.BET_TOPIC;
import static ru.curs.counting.model.TopicNames.EVENT_SCORE_TOPIC;
import static ru.curs.counting.model.TopicNames.FRAUD_TOPIC;
import static ru.curs.counting.model.TopicNames.TEAM_BET_TOPIC;
import static ru.curs.counting.model.TopicNames.USER_BET_TOPIC;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Fraud;
import ru.curs.counting.model.Outcome;
import ru.curs.homework.transformer.FakeBetTransformer;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {

  @Bean
  public Topology createTopology(StreamsBuilder streamsBuilder) {
    KStream<String, Bet> betsInput = streamsBuilder.stream(
        BET_TOPIC,
        Consumed.with(Serdes.String(), new JsonSerde<>(Bet.class))
            .withTimestampExtractor((r, p) -> ((Bet) r.value()).getTimestamp()));
    KStream<String, EventScore> scoresInput = streamsBuilder.stream(
        EVENT_SCORE_TOPIC,
        Consumed.with(Serdes.String(), new JsonSerde<>(EventScore.class))
            .withTimestampExtractor((r, p) -> ((EventScore) r.value()).getTimestamp()));

    // Task 1
    KTable<String, Long> user_bets = betsInput
        .map((k, v) -> KeyValue.pair(v.getBettor(), v.getAmount()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())).reduce(Long::sum);

    // Task 2
    KTable<String, Long> team_bets = betsInput
        .filter((k, v) -> v.getOutcome() != Outcome.D)
        .map((k, v) -> KeyValue.pair(
            v.getMatch().split("-")[v.getOutcome() == Outcome.H ? 0 : 1],
            v.getAmount()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())).reduce(Long::sum);

    // Task 3
    KStream<String, Bet> fakeBets = new FakeBetTransformer()
        .transformStream(streamsBuilder, scoresInput);

    KStream<String, Fraud> fraud_bets =
        betsInput.join(
            fakeBets,
            (b, w) -> Fraud.builder()
                .bettor(b.getBettor())
                .outcome(b.getOutcome())
                .amount(b.getAmount())
                .match(b.getMatch())
                .odds(b.getOdds())
                .lag(w.getTimestamp() - b.getTimestamp())
                .build(),
            JoinWindows.of(Duration.ofSeconds(1))
                .before(Duration.ZERO),
            StreamJoined
                .with(Serdes.String(), new JsonSerde<>(Bet.class), new JsonSerde<>(Bet.class)))
            .filter((k, v) -> v.getLag() <= 1)
            .map((k, v) -> KeyValue.pair(v.getBettor(), v));

    user_bets.toStream()
        .to(USER_BET_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    team_bets.toStream()
        .to(TEAM_BET_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    fraud_bets
        .to(FRAUD_TOPIC,
            Produced.with(Serdes.String(), new JsonSerde<>(Fraud.class)));

    Topology topology = streamsBuilder.build();
    System.out.println("========================================");
    System.out.println(topology.describe());
    System.out.println("========================================");
    // https://zz85.github.io/kafka-streams-viz/
    return topology;
  }

}
