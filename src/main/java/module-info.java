module com.vinicius.kafkamanager {
    requires javafx.controls;
    requires javafx.fxml;
    requires kafka.clients;
    requires static lombok;
    requires com.fasterxml.jackson.databind;


    opens com.vinicius.kafkamanager to javafx.fxml;
    exports com.vinicius.kafkamanager;
}