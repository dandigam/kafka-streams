package com.example.demo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloStream {

	private static final Logger logger = LoggerFactory.getLogger(HelloStream.class);

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.application_id);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstapServer);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> kStream = builder.stream(AppConfigs.topicName);
		kStream.foreach((key, value) -> System.out.print("Key = " + key + "Value =" + value));
		kStream.peek((key, value) -> System.out.print("Key = " + key + "Value =" + value));

		Topology topology = builder.build();

		KafkaStreams streams = new KafkaStreams(topology, props);

		logger.info("Stream started....");

		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shutdown Stream");
			streams.close();

		}));

	}

}
