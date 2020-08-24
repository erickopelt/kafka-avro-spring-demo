package io.opelt.kafkaspringdemo.controller;

import io.opelt.kafkaspringdemo.User;
import io.opelt.kafkaspringdemo.domain.AddressEntity;
import io.opelt.kafkaspringdemo.producer.UserProducer;
import io.opelt.kafkaspringdemo.repository.UserRepository;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/users")
public class UserController {

    private final UserProducer userProducer;
    private final UserRepository userRepository;

    public UserController(UserProducer userProducer, UserRepository userRepository) {
        this.userProducer = userProducer;
        this.userRepository = userRepository;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> create(@RequestBody UserDTO userDTO) {
        var id = UUID.randomUUID().toString();
        userProducer.sendMessage(User.newBuilder()
                .setId(id)
                .setName(userDTO.getName())
                .setBirthday(userDTO.getBirthday().toEpochMilli())
                .setAddresses(userDTO.getAddresses().stream().map(s -> (CharSequence) s).collect(Collectors.toList()))
                .build());
        var location = WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(UserController.class).getById(id)).toUri();
        return ResponseEntity.created(location).build();
    }

    @GetMapping(path = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<UserDTO> getById(@PathVariable String id) {
        return userRepository.findById(id)
                .map(user -> new UserDTO()
                        .setId(user.getId())
                        .setName(user.getName())
                        .setBirthday(user.getBirthday())
                        .setAddresses(user.getAddresses().stream().map(AddressEntity::getAddress).collect(Collectors.toList())))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

}
