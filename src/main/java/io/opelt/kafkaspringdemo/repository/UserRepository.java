package io.opelt.kafkaspringdemo.repository;

import io.opelt.kafkaspringdemo.domain.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<UserEntity, String> {
}
