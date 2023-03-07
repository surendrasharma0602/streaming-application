package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// @Slf4j
public class KTable {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.102.118:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // final StreamsBuilder builder = new StreamsBuilder();

        // builder.stream("mpi-update").to("mpi-cancelled");

        // final Topology topology = builder.build();
//         final Topology topology = filterMpiUpdateTopology("mpi-updates", "mpi-cancelled");
        final Topology topology = tableTopology( "price","final-price");
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    static Topology filterMpiUpdateTopology(String inputTopic, String outputTopic) {
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k, v) -> System.out.println("Processing mpi update event: " + v))
                .filter((k, v) -> v.contains("cancelled"))
                .peek((k, v) -> System.out.println("Transformed event: " + v))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        return builder.build();
    }

}