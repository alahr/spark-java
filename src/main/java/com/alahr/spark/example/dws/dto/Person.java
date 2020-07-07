package com.alahr.spark.example.dws.dto;

import java.io.Serializable;

public class Person implements Serializable {
    private static final long serialVersionUID = 1L;

    private String city;
    private String name;
    private String book;
    private Double price;

    public Person(){
        super();
    }

    public Person(String city, String name, String book, Double price){
        this();
        this.city = city;
        this.name = name;
        this.book = book;
        this.price = price;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBook() {
        return book;
    }

    public void setBook(String book) {
        this.book = book;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
