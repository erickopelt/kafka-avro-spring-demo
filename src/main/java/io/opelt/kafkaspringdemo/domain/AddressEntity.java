package io.opelt.kafkaspringdemo.domain;

import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;

@Entity
public class AddressEntity {

    @Id
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    private String id;
    @Column
    private String address;
    @ManyToOne
    private UserEntity user;

    public String getId() {
        return id;
    }

    public AddressEntity setId(String id) {
        this.id = id;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public AddressEntity setAddress(String address) {
        this.address = address;
        return this;
    }

    public UserEntity getUser() {
        return user;
    }

    public AddressEntity setUser(UserEntity user) {
        this.user = user;
        return this;
    }
}
