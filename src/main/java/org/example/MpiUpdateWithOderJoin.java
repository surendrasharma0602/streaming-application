package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// @Slf4j
public class MpiUpdateWithOderJoin {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mpiOrderStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        final Topology topology = mpiupdateWithOrderTopology("mpi-updates1","order-updates1","mpi_oder1");
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

    static Topology mpiupdateWithOrderTopology(String mpiUpdateTopic, String orderUpdateTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> mpiUpdateStream = builder.stream(mpiUpdateTopic);
        mpiUpdateStream.peek((k, v) -> System.out.println("mpi update received key: " +k + " value: " + v));
        KTable<String,String> orderTable = builder.table(
                orderUpdateTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("mpiEvent_Order"));
        orderTable.toStream().peek((k, v) -> System.out.println("order update received key: " + k + " value:" +v));

        mpiUpdateStream
                .join(orderTable, (mpiUpdate, order)->  mpiUpdate +" "+ order )
                .peek((k, v) -> System.out.println("Mpi_order: " + v))
                .to(outputTopic);
        return builder.build();
    }

}