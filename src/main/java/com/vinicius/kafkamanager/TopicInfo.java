package com.vinicius.kafkamanager;

import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleStringProperty;

// Atualize a classe TopicInfo para incluir novos campos
public class TopicInfo {
    private final SimpleStringProperty name;
    private final SimpleIntegerProperty partitions;
    private final SimpleIntegerProperty replicationFactor;
    private final SimpleLongProperty messageCount;
    private final SimpleStringProperty inSyncReplicas;

    public TopicInfo(String name, int partitions, int replicationFactor, long messageCount, String inSyncReplicas) {
        this.name = new SimpleStringProperty(name);
        this.partitions = new SimpleIntegerProperty(partitions);
        this.replicationFactor = new SimpleIntegerProperty(replicationFactor);
        this.messageCount = new SimpleLongProperty(messageCount);
        this.inSyncReplicas = new SimpleStringProperty(inSyncReplicas);
    }

    public SimpleStringProperty name() {
        return name;
    }

    public SimpleIntegerProperty partitions() {
        return partitions;
    }

    public SimpleIntegerProperty replicationFactor() {
        return replicationFactor;
    }

    public SimpleLongProperty messageCount() {
        return messageCount;
    }

    public SimpleStringProperty inSyncReplicas() {
        return inSyncReplicas;
    }
}

