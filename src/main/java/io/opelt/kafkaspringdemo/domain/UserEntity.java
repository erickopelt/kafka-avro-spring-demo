package io.opelt.kafkaspringdemo.domain;

import javax.persistence.*;
import java.time.Instant;
import java.util.List;

@Entity
public class UserEntity {

    @Id
    private String id;
    private String name;
    private Instant birthday;
    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    private List<AddressEntity> addresses;

    public String getId() {
        return id;
    }

    public UserEntity setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public UserEntity setName(String name) {
        this.name = name;
        return this;
    }

    public Instant getBirthday() {
        return birthday;
    }

    public UserEntity setBirthday(Instant birthday) {
        this.birthday = birthday;
        return this;
    }

    public List<AddressEntity> getAddresses() {
        return addresses;
    }

    public UserEntity setAddresses(List<AddressEntity> addresses) {
        this.addresses = addresses;
        return this;
    }
}
