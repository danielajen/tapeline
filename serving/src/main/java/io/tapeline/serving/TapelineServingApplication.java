package io.tapeline.serving;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Tapeline's serving tier: gRPC streaming, a REST gateway, and the hot cache. */
@SpringBootApplication
public class TapelineServingApplication {
    public static void main(String[] args) {
        SpringApplication.run(TapelineServingApplication.class, args);
    }
}
