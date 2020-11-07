package ru.curs.homework.transformer;

import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Outcome;
import ru.curs.counting.model.Score;

public class FakeBetTransformer implements
    StatefulTransformer<String, Score, String, EventScore, String, Bet> {

  @Override
  public String storeName() {
    return "event-score-cache";
  }

  @Override
  public Serde<String> storeKeySerde() {
    return Serdes.String();
  }

  @Override
  public Serde<Score> storeValueSerde() {
    return new JsonSerde<>(Score.class);
  }

  @Override
  public KeyValue<String, Bet> transform(String key, EventScore value,
      KeyValueStore<String, Score> stateStore) {
    Score prev = Optional.ofNullable(stateStore.get(key)).orElse(new Score());
    stateStore.put(key, value.getScore());
    Outcome out = value.getScore().getHome() > prev.getHome() ? Outcome.H : Outcome.A;
    Bet b = new Bet(null, value.getEvent(), out, 0, 1, value.getTimestamp());
    return KeyValue.pair(String.format("%s:%s", key, out), b);
  }
}
