package samples.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class Helper {

    private static final Logger log = LoggerFactory.getLogger(Helper.class);

    private KafkaContainer kafka;
    public String kafkaBootstrapServers;

    public Helper() {
    }

    public void startContainers() {
        kafka = new KafkaContainer("5.1.2"); // contains Kafka 2.1.x
        kafka.start();
        kafkaBootstrapServers = kafka.getBootstrapServers();
    }

    public void stopContainers() {
        kafka.stop();
    }

    CompletionStage<Done> writeToKafka(String topic, List<Movie> movies, ActorSystem actorSystem) {
        ProducerSettings<Integer, String> kafkaProducerSettings =
                ProducerSettings.create(actorSystem, new IntegerSerializer(), new StringSerializer())
                        .withBootstrapServers(kafkaBootstrapServers);

        CompletionStage<Done> producing =
                Source.from(movies)
                        .map(
                                movie -> {
                                    log.debug("producing {}", movie);
                                    String json = JsonMappers.movieWriter.writeValueAsString(movie);
                                    return new ProducerRecord<>(topic, movie.id, json);
                                })
                        .runWith(Producer.plainSink(kafkaProducerSettings), actorSystem);
        producing.thenAccept(s -> log.info("Producing finished"));
        return producing;
    }


}
