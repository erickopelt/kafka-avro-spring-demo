package io.opelt.kafkaspringdemo.controller;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.opelt.kafkaspringdemo.User;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.apache.avro.generic.GenericData;
import org.apache.http.HttpHeaders;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@EmbeddedKafka(topics = "users", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class UserProducerIT {

    @Value("${topic.name}")
    private String topicName;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @LocalServerPort
    private int port;

    private Map<String, User> records;
    private KafkaMessageListenerContainer<String, User> container;

    @BeforeEach
    public void setup() {
        RestAssured.port = port;

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("sender", "false", embeddedKafkaBroker));
        DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new KafkaAvroDeserializer(MockSchemaRegistry.getClientForScope("localhost:8081")));
        ContainerProperties containerProperties = new ContainerProperties(topicName);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new ConcurrentHashMap<>();
        container.setupMessageListener((MessageListener<String, GenericData.Record>) record ->
                records.put(record.key(),
                        User.newBuilder()
                                .setId((CharSequence) record.value().get("id"))
                                .setName((CharSequence) record.value().get("name"))
                                .setBirthday((Long) record.value().get("birthday"))
                                .setAddresses((List<CharSequence>) record.value().get("addresses"))
                                .build()));

        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
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

        var record = records.get(id.get());

        assertThat(record).isNotNull().satisfies(user -> {
            assertThat(user.getId()).isNotBlank();
            assertThat(user.getName().toString()).isEqualTo("erick");
            assertThat(Instant.ofEpochMilli(user.getBirthday()).atZone(ZoneId.of("UTC"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))).isEqualTo("2020-08-18T19:15:54.423Z");
            assertThat(user.getAddresses().stream().map(CharSequence::toString).collect(Collectors.toList())).hasSameElementsAs(List.of("Blumenau Street"));
        });
    }

    private Optional<String> getIdFromLocation(String location) {
        var matcher = Pattern.compile("http://localhost:.*/users/(?<id>.*)").matcher(location);
        if (matcher.find()) {
            return Optional.ofNullable(matcher.group("id"));
        }
        return Optional.empty();
    }

}
