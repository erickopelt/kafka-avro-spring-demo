package io.opelt.kafkaspringdemo.controller;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.opelt.kafkaspringdemo.User;
import io.restassured.RestAssured;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

@EmbeddedKafka(topics = "users", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class UserConsumerIT {

    @Value("${topic.name}")
    private String topicName;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @LocalServerPort
    private int port;

    private Producer<String, Object> producer;

    @BeforeEach
    public void setup() throws IOException, RestClientException {
        RestAssured.port = port;

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        SchemaRegistryClient clientForScope = MockSchemaRegistry.getClientForScope("localhost:8081");
        clientForScope.register("users-value", new AvroSchema(User.getClassSchema()));
        producer = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new KafkaAvroSerializer(clientForScope))
                .createProducer();
    }

    @Test
    public void whenCreateUserSendMessageToKafka() {
        var birthday = Instant.now();
        var user = User.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setName("erick")
                .setBirthday(birthday.toEpochMilli())
                .setAddresses(List.of("Blumenau Street"))
                .build();

        producer.send(new ProducerRecord<>(topicName, user.getId().toString(), user));

        given()
                .pathParam("id", user.getId())
                .when()
                .get("/users/{id}")
                .then()
                .statusCode(HttpStatus.OK.value())
                .body("id", equalTo(user.getId().toString()))
                .body("name", equalTo(user.getName().toString()))
                .body("addresses", contains("Blumenau Street"))
                .body("birthday", equalTo(birthday.atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))));
    }

}
