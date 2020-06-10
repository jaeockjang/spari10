package com.swjuyhz.sample.sparkstream.executor;

import java.io.Serializable;

public class KafkaData implements Serializable {
    private String key;
    private String value;

    public KafkaData(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
