package com.vinicius.kafkamanager;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class ProducerConfig {
    private String name;
    private String topic;
    private String key;
    private String valueTemplate;
    private Map<String, String> headers;
}