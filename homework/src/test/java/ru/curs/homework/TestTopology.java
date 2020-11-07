package ru.curs.homework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ru.curs.counting.model.TopicNames.BET_TOPIC;
import static ru.curs.counting.model.TopicNames.EVENT_SCORE_TOPIC;
import static ru.curs.counting.model.TopicNames.FRAUD_TOPIC;
import static ru.curs.counting.model.TopicNames.TEAM_BET_TOPIC;
import static ru.curs.counting.model.TopicNames.USER_BET_TOPIC;

import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Fraud;
import ru.curs.counting.model.Outcome;
import ru.curs.counting.model.Score;
import ru.curs.homework.configuration.KafkaConfiguration;
import ru.curs.homework.configuration.TopologyConfiguration;

public class TestTopology {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Bet> betInputTopic;
  private TestInputTopic<String, EventScore> scoreInputTopic;
  private TestOutputTopic<String, Long> userOutputTopic;
  private TestOutputTopic<String, Long> teamOutputTopic;
  private TestOutputTopic<String, Fraud> suspiciousOutputTopic;

  @Before
  public void setUp() {
    KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
    StreamsBuilder sb = new StreamsBuilder();
    testDriver = new TopologyTestDriver(
        new TopologyConfiguration().createTopology(sb), config.asProperties());

    betInputTopic = testDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(),
        new JsonSerde<>(Bet.class).serializer());
    scoreInputTopic = testDriver.createInputTopic(EVENT_SCORE_TOPIC, Serdes.String().serializer(),
        new JsonSerde<>(EventScore.class).serializer());

    userOutputTopic = testDriver
        .createOutputTopic(USER_BET_TOPIC, Serdes.String().deserializer(),
            Serdes.Long().deserializer());
    teamOutputTopic = testDriver
        .createOutputTopic(TEAM_BET_TOPIC, Serdes.String().deserializer(),
            Serdes.Long().deserializer());
    suspiciousOutputTopic = testDriver
        .createOutputTopic(FRAUD_TOPIC, Serdes.String().deserializer(),
            new JsonSerde<>(Fraud.class).deserializer());
  }

  @After
  public void tearDown() {
    testDriver.close();
  }

  private void insertBet(Bet v) {
    betInputTopic.pipeInput(v.key(), v);
  }

  private void insertEventScore(EventScore s) {
    scoreInputTopic.pipeInput(s.getEvent(), s);
  }

  @Test
  public void testUserTopology() {
    Bet bet1 = Bet.builder()
        .bettor("John Doe")
        .match("Germany-Belgium")
        .outcome(Outcome.H)
        .amount(100)
        .odds(1.7).build();

    Bet bet2 = Bet.builder()
        .bettor("Ivan Petrov")
        .match("Russia-Germany")
        .outcome(Outcome.A)
        .amount(180)
        .odds(4.3).build();

    Bet bet3 = Bet.builder()
        .bettor("John Doe")
        .match("Belgium-Germany")
        .outcome(Outcome.D)
        .amount(230)
        .odds(1.9).build();

    insertBet(bet1);
    TestRecord<String, Long> record = userOutputTopic.readRecord();
    assertEquals(bet1.getBettor(), record.key());
    assertEquals(bet1.getAmount(), record.value().longValue());

    insertBet(bet2);
    record = userOutputTopic.readRecord();
    assertEquals(bet2.getBettor(), record.key());
    assertEquals(bet2.getAmount(), record.value().longValue());

    insertBet(bet3);
    record = userOutputTopic.readRecord();
    assertEquals(bet3.getBettor(), record.key());
    assertEquals(bet1.getAmount() + bet3.getAmount(), record.value().longValue());
  }

  @Test
  public void testTeamTopology() {
    Bet bet1 = Bet.builder()
        .bettor("John Doe")
        .match("Germany-Belgium")
        .outcome(Outcome.H)
        .amount(100)
        .odds(1.7).build();

    Bet bet2 = Bet.builder()
        .bettor("John Doe")
        .match("Belgium-Germany")
        .outcome(Outcome.D)
        .amount(230)
        .odds(1.9).build();

    Bet bet3 = Bet.builder()
        .bettor("Ivan Petrov")
        .match("Russia-Germany")
        .outcome(Outcome.A)
        .amount(180)
        .odds(4.3).build();

    Bet bet4 = Bet.builder()
        .bettor("John Doe")
        .match("Russia-Belgium")
        .outcome(Outcome.A)
        .amount(230)
        .odds(1.9).build();

    insertBet(bet1);
    TestRecord<String, Long> record = teamOutputTopic.readRecord();
    assertEquals(record.key(), "Germany");
    assertEquals(record.value().longValue(), 100L);

    insertBet(bet2);
    assertTrue(teamOutputTopic.isEmpty());

    insertBet(bet3);
    record = teamOutputTopic.readRecord();
    assertEquals(record.key(), "Germany");
    assertEquals(record.value().longValue(), 280L);

    insertBet(bet4);
    record = teamOutputTopic.readRecord();
    assertEquals(record.key(), "Belgium");
    assertEquals(record.value().longValue(), 230L);
  }

  @Test
  public void testSuspiciousTopology() {
    Bet bet1 = Bet.builder()
        .bettor("John Doe")
        .match("Germany-Belgium")
        .outcome(Outcome.H)
        .amount(100)
        .timestamp(1904761885L)
        .odds(1.7).build();

    Bet bet2 = Bet.builder()
        .bettor("Ivan Ivanov")
        .match("Germany-Belgium")
        .outcome(Outcome.A)
        .amount(100)
        .timestamp(1904761995L)
        .odds(1.7).build();

    Bet bet3 = Bet.builder()
        .bettor("Petr Ivanov")
        .match("Germany-Belgium")
        .outcome(Outcome.A)
        .amount(100)
        .timestamp(1904761993L)
        .odds(1.7).build();

    Bet bet4 = Bet.builder()
        .bettor("Ivan Petrov")
        .match("Germany-Belgium")
        .outcome(Outcome.A)
        .amount(100)
        .timestamp(1904761996L)
        .odds(1.7).build();

    Bet bet5 = Bet.builder()
        .bettor("Petr Petrov")
        .match("Germany-Belgium")
        .outcome(Outcome.H)
        .amount(100)
        .timestamp(1904761995L)
        .odds(1.7).build();

    EventScore e1 = new EventScore("Germany-Belgium", new Score(1, 0), 1904761886L);
    EventScore e2 = new EventScore("Germany-Belgium", new Score(1, 1), 1904761995L);

    insertBet(bet1);
    insertBet(bet3);
    insertBet(bet2);
    insertBet(bet5);
    insertBet(bet4);

    insertEventScore(e1);
    insertEventScore(e2);

    val record = suspiciousOutputTopic.readRecordsToList();

    assertEquals(2, record.size());
    assertEquals(bet1.getBettor(), record.get(0).key());
    assertEquals(bet2.getBettor(), record.get(1).key());
  }

}
