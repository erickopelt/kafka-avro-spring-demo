package io.opelt.kafkaspringdemo.consumer;

import io.opelt.kafkaspringdemo.User;
import io.opelt.kafkaspringdemo.domain.AddressEntity;
import io.opelt.kafkaspringdemo.domain.UserEntity;
import io.opelt.kafkaspringdemo.repository.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.stream.Collectors;

@Component
public class UserConsumer {

    private static final Logger logger = LoggerFactory.getLogger(UserConsumer.class);
    private final UserRepository userRepository;

    public UserConsumer(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @KafkaListener(topics = "${topic.name}")
    public void consume(ConsumerRecord<String, User> record, Acknowledgment ack) {
        var user = record.value();
        logger.info(user.toString());
        UserEntity userEntity = new UserEntity()
                .setId(user.getId().toString())
                .setName(user.getName().toString())
                .setBirthday(Instant.ofEpochMilli(user.getBirthday()));
        userEntity.setAddresses(user.getAddresses().stream()
                .map(address -> new AddressEntity()
                        .setAddress(address.toString())
                        .setUser(userEntity))
                .collect(Collectors.toList()));
        userRepository.save(userEntity);
        ack.acknowledge();
    }
}
