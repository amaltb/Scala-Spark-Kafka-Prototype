package com.ab.example.sparkstructuredstreamwithkafka.producer.util;

public class Request {

    private int id;
    private Double latitude;
    private Double longitude;

    public Request() {
    }

    public Request(int id, Double latitude, Double longitude) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    @Override
    public String toString() {
        return String.valueOf(id) + "," + String.valueOf(latitude) + "," + String.valueOf(longitude);
    }
}
