package com.example.demobatch.model;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Autobot {

	@Id
	private String name;
    private String car;

    public Autobot() {
    }

    public Autobot(String name, String car) {
        this.name = name;
        this.car = car;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCar() {
        return car;
    }

    public void setCar(String car) {
        this.car = car;
    }
}
