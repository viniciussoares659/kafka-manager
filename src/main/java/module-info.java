module com.vinicius.kafkamanager {
    requires javafx.controls;
    requires javafx.fxml;
    requires kafka.clients;
    requires static lombok;


    opens com.vinicius.kafkamanager to javafx.fxml;
    exports com.vinicius.kafkamanager;
}