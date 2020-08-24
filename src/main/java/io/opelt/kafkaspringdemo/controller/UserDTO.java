package io.opelt.kafkaspringdemo.controller;

import java.time.Instant;
import java.util.List;

public class UserDTO {

    private String id;
    private String name;
    private Instant birthday;
    private List<String> addresses;

    public String getId() {
        return id;
    }

    public UserDTO setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public UserDTO setName(String name) {
        this.name = name;
        return this;
    }

    public Instant getBirthday() {
        return birthday;
    }

    public UserDTO setBirthday(Instant birthday) {
        this.birthday = birthday;
        return this;
    }

    public List<String> getAddresses() {
        return addresses;
    }

    public UserDTO setAddresses(List<String> addresses) {
        this.addresses = addresses;
        return this;
    }
}
