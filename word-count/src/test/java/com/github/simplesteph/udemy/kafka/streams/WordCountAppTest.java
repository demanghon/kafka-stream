package com.github.simplesteph.udemy.kafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

public class WordCountAppTest {

    TopologyTestDriver testDriver;

    StringSerde stringSerde = new StringSerde();
    LongSerde longSerde = new LongSerde();

    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, Long> outputTopic;


    @BeforeEach
    public void setUpTopologyTestDriver(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic("word-count-input", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("word-count-output", stringSerde.deserializer(), longSerde.deserializer());
    }

    @AfterEach
    public void closeTestDriver(){
        testDriver.close();
    }

    public void pushNewInputRecord(String value){
        inputTopic.pipeInput(null, value);
    }

    @Test
    public void dummyTest(){
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("testing", 1L));
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("kafka", 1L));
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("streams", 1L));

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("testing", 2L));
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("kafka", 2L));
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("again", 1L));
        assertEquals(outputTopic.isEmpty(), true);

    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("kafka", 1L));
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("kafka", 2L));
        assertEquals(outputTopic.readKeyValue(),  new KeyValue<>("kafka", 3L));
        assertEquals(outputTopic.isEmpty(), true);
    }
}
