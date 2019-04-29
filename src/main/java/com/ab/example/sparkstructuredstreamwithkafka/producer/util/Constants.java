package com.ab.example.sparkstructuredstreamwithkafka.producer.util;

public class Constants {

    public static final String SUPPLY_TOPIC = "car_supply";
    public static final String DEMAND_TOPIC = "car_demand";
    public static final Double GEO_LOC_START_LONG = 77.419880; //77.3924088
    public static final Double GEO_LOC_END_LONG = 77.771506;  // 77.8138004
    public static final Double GEO_LOC_START_LAT = 12.752921; //12.695278
    public static final Double GEO_LOC_END_LAT = 13.160000;   //13.2166061

    public static final int MAX_KAFKA_PRODUCER_RETRIES = 3;
}
