package io.opelt.kafkaspringdemo.controller;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.opelt.kafkaspringdemo.User;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.jayway.jsonpath.internal.path.PathCompiler.fail;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = "${topic.name}", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class UserProducerIT {

    @Value("${topic.name}")
    private String topic;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @LocalServerPort
    private int port;
    private final BlockingQueue<ConsumerRecord> records = new LinkedBlockingQueue<>();
    private KafkaMessageListenerContainer<String, Object> container;

    @BeforeEach
    public void setup() {
        RestAssured.port = port;

        var configs = new HashMap<>(KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "false", embeddedKafkaBroker));
        var consumerFactory =
                new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new KafkaAvroDeserializer(MockSchemaRegistry.getClientForScope("localhost:8081")));
        ContainerProperties containerProperties = new ContainerProperties(topic);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setupMessageListener((MessageListener<String, ConsumerRecord>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

        records.clear();
    }

    @AfterEach
    public void tearDown() {
        container.stop();
    }

    @Test
    public void whenCreateUserSendMessageToKafka() throws Exception {
        var userJson = new JSONObject()
                .put("name", "erick")
                .put("birthday", "2020-08-18T19:15:54.423Z")
                .put("addresses", new JSONArray().put("Blumenau Street"));

        var location = given()
                .contentType(ContentType.JSON)
                .body(userJson.toString())
                .when()
                .post("/users")
                .then()
                .statusCode(HttpStatus.CREATED.value())
                .extract()
                .header(HttpHeaders.LOCATION);
        var id = getIdFromLocation(location);
        if (id.isEmpty()) {
            fail("Can't parse location header");
        }

        var record = records.poll(1, TimeUnit.SECONDS);

        assertThat(record).isNotNull().satisfies(genericRecord -> {
            var user = (User) SpecificData.get().deepCopy(User.SCHEMA$, genericRecord.value());
            assertThat(user.getId().toString()).isEqualTo(id.get());
            assertThat(user.getName().toString()).isEqualTo("erick");
            assertThat(Instant.ofEpochMilli(user.getBirthday()).atZone(ZoneId.of("UTC"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))).isEqualTo("2020-08-18T19:15:54.423Z");
            assertThat(user.getAddresses().stream().map(CharSequence::toString).collect(Collectors.toList())).hasSameElementsAs(List.of("Blumenau Street"));
        });
    }

    private Optional<String> getIdFromLocation(String location) {
        var matcher = Pattern.compile("http://localhost:.*/users/(?<id>.*)").matcher(location);
        if (matcher.find()) {
            return Optional.ofNullable(matcher.group("id")).filter(id -> !id.isBlank());
        }
        return Optional.empty();
    }
}
