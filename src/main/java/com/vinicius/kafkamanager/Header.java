package com.vinicius.kafkamanager;

import javafx.beans.property.SimpleStringProperty;

public class Header {
    private SimpleStringProperty key;
    private SimpleStringProperty value;

    public Header(String key, String value) {
        this.key = new SimpleStringProperty(key);
        this.value = new SimpleStringProperty(value);
    }

    public SimpleStringProperty keyProperty() { return key; }
    public SimpleStringProperty valueProperty() { return value; }

    public String getKey() {
        return key.get();
    }

    public SimpleStringProperty key() {
        return key;
    }

    public String getValue() {
        return value.get();
    }

    public SimpleStringProperty value() {
        return value;
    }

    public void setKey(String key) {
        this.key.set(key);
    }

    public void setValue(String value) {
        this.value.set(value);
    }
}
