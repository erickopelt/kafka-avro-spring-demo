package io.opelt.kafkaspringdemo.producer;

import io.opelt.kafkaspringdemo.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class UserProducer {

    private final String topicName;
    private final KafkaTemplate<String, User> kafkaTemplate;

    public UserProducer(KafkaTemplate<String, User> kafkaTemplate, @Value("${topic.name}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void sendMessage(User user) {
        kafkaTemplate.send(topicName, (String) user.getId(), user);
    }
}
