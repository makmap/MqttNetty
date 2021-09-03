package com.app.mqtt.netty;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MqttNettyApplication {

    public static void main(String[] args) {
        SpringApplication.run(MqttNettyApplication.class, args);
        // 启动  1883
        new BootNettyServer().startup();
    }

}
