package com.vinicius.kafkamanager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.*;
import javafx.stage.Stage;
import javafx.util.Duration;
import javafx.util.StringConverter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaApp extends Application {

    private static final String CONFIG_FILE = "producer_configs.json";

    private CompletableFuture<Void> connectionFuture;

    @Override
    public void init() {
        KafkaConnectionPool.startHealthCheck(120);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            KafkaConnectionPool.shutdown();
            System.out.println("Conexões e scheduler foram encerrados");
        }));
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Kafka Manager");

        ImageView kafkaIcon = new ImageView(
                new Image(Objects.requireNonNull(getClass().getResource("/kafka-background.png")).toExternalForm())
        );
        kafkaIcon.setFitWidth(100);
        kafkaIcon.setPreserveRatio(true);

        Label kafkaLabel = new Label("Kafka Host:");
        TextField kafkaField = new TextField();
        kafkaField.setPromptText("Digite o endereço do servidor Kafka (e.g. localhost:9092)");
        kafkaField.setText("localhost:9092");

        Button connectButton = new Button("Conectar");
        Button cancelButton = new Button("Cancelar");
        cancelButton.setId("cancelButton");
        cancelButton.setDisable(true);

        kafkaField.setOnKeyPressed(event -> {
            if (Objects.requireNonNull(event.getCode()) == KeyCode.ENTER) {
                connectButton.fire();
            }
        });

        ProgressIndicator loader = new ProgressIndicator();
        loader.setVisible(false);

        connectButton.setOnAction(e -> {
            String kafkaServer = kafkaField.getText();
            connectButton.setDisable(true);
            cancelButton.setDisable(false);
            loader.setVisible(true);

            if (!kafkaServer.isEmpty()) {
                connectionFuture = CompletableFuture.supplyAsync(() -> testKafkaConnection(kafkaServer))
                        .thenAccept(isConnected -> Platform.runLater(() -> {
                            connectButton.setDisable(false);
                            cancelButton.setDisable(true);
                            loader.setVisible(false);

                            if (isConnected) {
                                openTopicListView(kafkaServer, primaryStage);
                            } else {
                                showAlert(Alert.AlertType.ERROR, "Erro na Conexão", "Falha ao conectar ao servidor Kafka.");
                            }
                        }))
                        .exceptionally(ex -> {
                            Platform.runLater(() -> {
                                connectButton.setDisable(false);
                                cancelButton.setDisable(true);
                                loader.setVisible(false);
                                showAlert(Alert.AlertType.ERROR, "Erro na Conexão", "Operação cancelada ou falhou: " + ex.getMessage());
                            });
                            return null;
                        });
            } else {
                connectButton.setDisable(false);
                cancelButton.setDisable(true);
                loader.setVisible(false);
                showAlert(Alert.AlertType.WARNING, "Campo vazio", "Por favor, insira o endereço do servidor Kafka.");
            }
        });

        cancelButton.setOnAction(e -> {
            if (connectionFuture != null && !connectionFuture.isDone()) {
                connectionFuture.cancel(true);
            }
            connectButton.setDisable(false);
            cancelButton.setDisable(true);
            loader.setVisible(false);
            showAlert(Alert.AlertType.INFORMATION, "Cancelado", "Tentativa de conexão cancelada.");
        });

        HBox buttonBox = new HBox(15, connectButton, cancelButton, loader);
        buttonBox.setAlignment(Pos.CENTER);

        VBox vbox = new VBox(20, kafkaIcon, kafkaLabel, kafkaField, buttonBox);
        vbox.setAlignment(Pos.CENTER);
        vbox.setPadding(new Insets(20));

        Scene scene = new Scene(vbox, 400, 300);
        scene.getStylesheets().add(Objects.requireNonNull(getClass().getResource("/styles.css")).toExternalForm());
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private void openTopicListView(String kafkaServer, Stage primaryStage) {
        BorderPane layout = new BorderPane();

        HBox menu = new HBox(10);
        menu.setPadding(new Insets(10));
        menu.setStyle("-fx-background-color: #f0f0f0;");
        menu.setAlignment(Pos.CENTER_LEFT);

        Button topicsButton = new Button("Tópicos");
        Button producersButton = new Button("Producers");
        Button exitButton = new Button("Sair");
        exitButton.setStyle("-fx-background-color: red; -fx-text-fill: white;");
        exitButton.setOnAction(e -> primaryStage.close());

        menu.getChildren().addAll(topicsButton, producersButton);

        HBox rightSide = new HBox(exitButton);
        rightSide.setAlignment(Pos.CENTER_RIGHT);
        menu.getChildren().add(rightSide);

        layout.setTop(menu);

        Label placeholderLabel = new Label("Selecione uma opção no menu.");
        placeholderLabel.setStyle("-fx-font-size: 16; -fx-text-fill: gray;");
        layout.setCenter(placeholderLabel);

        topicsButton.setOnAction(e -> showTopicsView(kafkaServer, layout));

        producersButton.setOnAction(e -> showProducersForm(kafkaServer, layout));

        Label footerLabel = new Label("Conectado ao host: " + kafkaServer);
        footerLabel.setStyle("-fx-text-fill: black;");

        Label statusLabel = new Label();
        statusLabel.setStyle("-fx-text-fill: green;");
        updateConnectionStatus(kafkaServer, statusLabel);

        HBox footer = new HBox(10, footerLabel, statusLabel);

        Timeline timeline = new Timeline(
                new KeyFrame(Duration.seconds(30),
                        e -> updateConnectionStatus(kafkaServer, statusLabel))
        );
        timeline.setCycleCount(Animation.INDEFINITE);
        timeline.play();
        footer.setPadding(new Insets(10));
        footer.setStyle("-fx-background-color: #ddd;");
        footer.setAlignment(Pos.CENTER_LEFT);

        HBox.setHgrow(footerLabel, Priority.ALWAYS);
        footer.setAlignment(Pos.CENTER_LEFT);

        layout.setBottom(footer);

        Scene scene = new Scene(layout, 800, 600);
        primaryStage.setScene(scene);
    }

    private boolean testKafkaConnection(String kafkaServer) {
        try {
            Admin admin = KafkaConnectionPool.getAdminClient(kafkaServer);
            admin.describeCluster().nodes().get(10, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            showAlert(Alert.AlertType.ERROR, "Erro na Conexão", "Detalhes: " + e.getMessage());
            return false;
        }
    }

    private void showTopicsView(String kafkaServer, BorderPane layout) {
        TableView<TopicInfo> table = new TableView<>();

        TableColumn<TopicInfo, String> nameCol = new TableColumn<>("Tópico");
        nameCol.setCellValueFactory(data -> data.getValue().name());

        TableColumn<TopicInfo, Long> messageCountCol = new TableColumn<>("Mensagem Count");
        messageCountCol.setCellValueFactory(data -> data.getValue().messageCount().asObject());

        TableColumn<TopicInfo, Integer> partitionsCol = new TableColumn<>("Partições");
        partitionsCol.setCellValueFactory(data -> data.getValue().partitions().asObject());

        TableColumn<TopicInfo, Integer> replicationFactorCol = new TableColumn<>("Fator de Replicação");
        replicationFactorCol.setCellValueFactory(data -> data.getValue().replicationFactor().asObject());

        TableColumn<TopicInfo, String> inSyncReplicasCol = new TableColumn<>("In Sync Replicas");
        inSyncReplicasCol.setCellValueFactory(data -> data.getValue().inSyncReplicas());

        table.getColumns().addAll(nameCol, messageCountCol, partitionsCol, replicationFactorCol, inSyncReplicasCol);
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        table.setStyle("-fx-font-size: 14px;");

        Label loadingLabel = new Label("Carregando dados...");
        loadingLabel.setStyle("-fx-font-size: 14px; -fx-text-fill: gray;");

        Button refreshButton = new Button("Atualizar");
        refreshButton.setOnAction(e -> loadTopics(kafkaServer, table, loadingLabel));

        HBox topicsButtons = new HBox(10);
        Button createTopicButton = new Button("Criar Tópico");
        createTopicButton.setOnAction(e -> showCreateTopicDialog(kafkaServer, table));
        topicsButtons.getChildren().addAll(refreshButton, createTopicButton);

        VBox vbox = new VBox(10, topicsButtons, table);
        vbox.setPadding(new Insets(10));

        layout.setCenter(vbox);

        loadTopics(kafkaServer, table, loadingLabel);
    }

    private void loadTopics(String kafkaServer, TableView<TopicInfo> table, Label loadingLabel) {
        table.getItems().clear();
        table.setPlaceholder(loadingLabel);

        CompletableFuture.runAsync(() -> {
            List<TopicInfo> topics = fetchTopicDetails(kafkaServer);

            Platform.runLater(() -> {
                if (topics.isEmpty()) {
                    table.setPlaceholder(new Label("Nenhum tópico disponível."));
                } else {
                    table.getItems().setAll(topics);
                }
            });
        });
    }

    private void showCreateTopicDialog(String kafkaServer, TableView<TopicInfo> table) {
        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Criar Novo Tópico");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);

        TextField nameField = new TextField();
        TextField partitionsField = new TextField();
        TextField replicationField = new TextField();

        partitionsField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.matches("\\d*")) {
                partitionsField.setText(newVal.replaceAll("[^\\d]", ""));
            }
        });

        replicationField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.matches("\\d*")) {
                replicationField.setText(newVal.replaceAll("[^\\d]", ""));
            }
        });

        grid.addRow(0, new Label("Nome:"), nameField);
        grid.addRow(1, new Label("Partições:"), partitionsField);
        grid.addRow(2, new Label("Fator de Replicação:"), replicationField);

        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        Optional<ButtonType> result = dialog.showAndWait();
        result.ifPresent(buttonType -> {
            if (buttonType == ButtonType.OK) {
                createTopic(
                        kafkaServer,
                        nameField.getText(),
                        partitionsField.getText(),
                        replicationField.getText(),
                        table
                );
            }
        });
    }

    private void createTopic(String kafkaServer, String name, String partitionsStr, String replicationStr, TableView<TopicInfo> table) {
        try {
            int partitions = Integer.parseInt(partitionsStr);
            short replication = Short.parseShort(replicationStr);

            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

            try (Admin admin = Admin.create(props)) {
                admin.createTopics(Collections.singletonList(
                        new NewTopic(name, partitions, replication)
                )).all().get(10, TimeUnit.SECONDS);

                loadTopics(kafkaServer, table, new Label());
                showAlert(Alert.AlertType.INFORMATION, "Sucesso", "Tópico criado com sucesso!");
            }
        } catch (Exception e) {
            showAlert(Alert.AlertType.ERROR, "Erro", "Falha ao criar tópico: " + e.getMessage());
        }
    }

    private void showProducersForm(String kafkaServer, BorderPane layout) {
        VBox form = new VBox(10);
        form.setPadding(new Insets(10));

        Label topicLabel = new Label("Selecione o Tópico:");
        ComboBox<String> topicComboBox = new ComboBox<>();
        topicComboBox.setEditable(true);

        ObservableList<String> allTopics = FXCollections.observableArrayList();
        topicComboBox.setItems(allTopics);
        topicComboBox.setMaxWidth(Double.MAX_VALUE);

        CompletableFuture.runAsync(() -> {
            List<String> topics = fetchTopics(kafkaServer);
            Platform.runLater(() -> {
                allTopics.setAll(topics);
                topicComboBox.setItems(allTopics);
            });
        });
        topicComboBox.setMaxWidth(Double.MAX_VALUE);

        topicComboBox.getEditor().textProperty().addListener((obs, oldValue, newValue) -> {
            topicComboBox.getSelectionModel().clearSelection();
            // Se o campo estiver vazio, exibe todos os tópicos
            if (newValue == null || newValue.isEmpty()) {
                topicComboBox.setItems(allTopics);
                topicComboBox.hide(); // Esconde para atualizar a lista
                topicComboBox.show(); // Mostra novamente
            } else {
                final String userInput = newValue.toLowerCase();
                // Filtra a lista completa conforme o input
                List<String> filtered = allTopics.stream()
                        .filter(topic -> topic.toLowerCase().contains(userInput))
                        .collect(Collectors.toList());
                topicComboBox.setItems(FXCollections.observableArrayList(filtered));
                topicComboBox.hide();
                topicComboBox.show();
            }
        });

        Label keyLabel = new Label("Chave:");
        TextField keyField = new TextField();
        keyField.setPromptText("Digite a chave (opcional)");
        keyField.setMaxWidth(Double.MAX_VALUE);
        keyField.setTooltip(new Tooltip("Chave opcional para particionamento"));

        Label valueLabel = new Label("Valor:");
        TextArea valueArea = new TextArea();
        valueArea.setPromptText("Digite o valor em JSON");
        valueArea.setMaxWidth(Double.MAX_VALUE);

        Label headersLabel = new Label("Headers:");
        TableView<Header> headersTable = new TableView<>();

        TableColumn<Header, String> headerKeyColumn = new TableColumn<>("Chave");
        headerKeyColumn.setCellValueFactory(data -> data.getValue().keyProperty());
        headerKeyColumn.setCellFactory(column -> new AutoCommitTextFieldTableCell<>());
        headerKeyColumn.prefWidthProperty().bind(headersTable.widthProperty().multiply(0.5));

        TableColumn<Header, String> headerValueColumn = new TableColumn<>("Valor");
        headerValueColumn.setCellValueFactory(data -> data.getValue().valueProperty());
        headerValueColumn.setCellFactory(column -> new AutoCommitTextFieldTableCell<>());
        headerValueColumn.prefWidthProperty().bind(headersTable.widthProperty().multiply(0.5));

        headersTable.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);

        Button addHeaderButton = new Button("+");
        Button removeHeaderButton = new Button("-");

        headersTable.setEditable(true);
        headersTable.getColumns().addAll(headerKeyColumn, headerValueColumn);

        addHeaderButton.setStyle("-fx-font-weight: bold; -fx-min-width: 25; -fx-min-height: 25;");
        removeHeaderButton.setStyle("-fx-font-weight: bold; -fx-min-width: 25; -fx-min-height: 25; -fx-background-color: #ff6666;");
        headersTable.setStyle("-fx-selection-bar: #e0f0ff; -fx-selection-bar-non-focused: #f0f0f0;");

        addHeaderButton.setOnAction(e -> {
            Header newHeader = new Header("", "");
            headersTable.getItems().add(newHeader);

            Platform.runLater(() -> {
                int lastIndex = headersTable.getItems().size() - 1;

                headersTable.getSelectionModel().select(lastIndex);
                headersTable.scrollTo(lastIndex);

                TablePosition<Header, ?> pos = new TablePosition<>(headersTable, lastIndex, headerKeyColumn);
                headersTable.getFocusModel().focus(pos.getRow(), pos.getTableColumn());

                headersTable.edit(pos.getRow(), pos.getTableColumn());
            });
        });

        HBox headersButtons = new HBox(5, addHeaderButton, removeHeaderButton);
        headersButtons.setAlignment(Pos.CENTER_LEFT);
        headersButtons.setPadding(new Insets(5, 0, 5, 0));

        removeHeaderButton.setOnAction(e -> {
            Header selectedHeader = headersTable.getSelectionModel().getSelectedItem();
            if (selectedHeader != null) {
                headersTable.getItems().remove(selectedHeader);
            }
        });

        Button sendButton = new Button("Enviar");
        sendButton.setOnAction(e -> {
            String topic = topicComboBox.getValue();
            String key = keyField.getText();
            String value = valueArea.getText();

            if (topic == null || topic.isEmpty()) {
                showAlert(Alert.AlertType.WARNING, "Erro", "Selecione um tópico.");
                return;
            }
            if (value.isEmpty()) {
                showAlert(Alert.AlertType.WARNING, "Erro", "Digite o valor (em JSON).");
                return;
            }

            Map<String, String> headers = new HashMap<>();
            for (Header header : headersTable.getItems()) {
                headers.put(header.getKey(), header.getValue());
            }

            CompletableFuture.runAsync(() -> {
                boolean success = sendMessage(kafkaServer, topic, key, value, headers);
                Platform.runLater(() -> {
                    if (success) {
                        showAlert(Alert.AlertType.INFORMATION, "Sucesso", "Mensagem enviada para o tópico " + topic + "!");
                        valueArea.clear();
                        keyField.clear();
                        headersTable.getItems().clear();
                    } else {
                        showAlert(Alert.AlertType.ERROR, "Erro", "Falha ao enviar a mensagem.");
                    }
                });
            });
        });

        valueArea.setPromptText("Digite o valor em JSON");
        HBox jsonTools = getHBox(valueArea);
        VBox valueBox = new VBox(5, valueLabel, valueArea, jsonTools);

        // Novo componente para histórico
        ComboBox<ProducerConfig> configCombo = new ComboBox<>();
        configCombo.setPromptText("Selecione uma configuração salva");
        configCombo.setConverter(new StringConverter<>() {
            @Override
            public String toString(ProducerConfig config) {
                return config != null ? config.getName() : "";
            }

            @Override
            public ProducerConfig fromString(String string) {
                return null;
            }
        });

        Button saveConfigButton = new Button("Salvar Configuração");
        Button deleteConfigButton = new Button("Excluir");

        HBox configTools = new HBox(10, configCombo, saveConfigButton, deleteConfigButton);

        saveConfigButton.setOnAction(e -> {
            TextInputDialog dialog = new TextInputDialog("Nova Configuração");
            dialog.setTitle("Salvar Configuração");
            dialog.setHeaderText("Digite um nome para esta configuração:");

            Optional<String> result = dialog.showAndWait();
            result.ifPresent(name -> {
                ProducerConfig config = new ProducerConfig(
                        name,
                        topicComboBox.getValue(),
                        keyField.getText(),
                        valueArea.getText(),
                        new HashMap<>()
                );

                headersTable.getItems().forEach(header ->
                        config.getHeaders().put(header.getKey(), header.getValue())
                );

                configCombo.getItems().add(config);
                saveConfigToFile(config);
            });
        });
        
        configCombo.setOnAction(e -> {
            ProducerConfig config = configCombo.getSelectionModel().getSelectedItem();
            if (config != null) {
                topicComboBox.setValue(config.getTopic());
                keyField.setText(config.getKey());
                valueArea.setText(config.getValueTemplate());

                headersTable.getItems().clear();
                config.getHeaders().forEach((k, v) ->
                        headersTable.getItems().add(new Header(k, v))
                );
            }
        });

        configCombo.setCellFactory(lv -> {
            ListCell<ProducerConfig> cell = new ListCell<>();
            ContextMenu contextMenu = new ContextMenu();

            MenuItem editItem = new MenuItem("Editar");
            editItem.setOnAction(event -> {
                ProducerConfig config = cell.getItem();
                openConfigEditor(config);
            });

            MenuItem deleteItem = new MenuItem("Excluir");
            deleteItem.setOnAction(event -> {
                ProducerConfig config = cell.getItem();
                configCombo.getItems().remove(config);
                deleteConfigFromFile(config);
            });

            contextMenu.getItems().addAll(editItem, deleteItem);

            cell.emptyProperty().addListener((obs, wasEmpty, isNowEmpty) -> {
                if (isNowEmpty) {
                    cell.setContextMenu(null);
                } else {
                    cell.setContextMenu(contextMenu);
                }
            });
            return cell;
        });

        form.getChildren().addAll(
                configTools,
                topicLabel, topicComboBox,
                keyLabel, keyField,
                valueBox,
                headersLabel, headersButtons, headersTable,
                sendButton
        );

        layout.setCenter(form);
    }

    private void deleteConfigFromFile(ProducerConfig config) {
    }

    private void openConfigEditor(ProducerConfig config) {
    }

    private HBox getHBox(TextArea valueArea) {
        Button formatJsonButton = new Button("Formatar");
        formatJsonButton.setTooltip(new Tooltip("Formatar e validar JSON"));
        formatJsonButton.setOnAction(e -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                Object json = mapper.readValue(valueArea.getText(), Object.class);
                String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
                valueArea.setText(prettyJson);
            } catch (Exception ex) {
                showAlert(Alert.AlertType.ERROR, "JSON Inválido", "O texto não é um JSON válido");
            }
        });

        HBox jsonTools = new HBox(5, formatJsonButton);
        jsonTools.setAlignment(Pos.CENTER_RIGHT);
        return jsonTools;
    }


    private List<TopicInfo> fetchTopicDetails(String kafkaServer) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        try (Admin admin = Admin.create(props)) {
            List<TopicInfo> topics = new ArrayList<>();
            admin.describeTopics(admin.listTopics().names().get())
                    .allTopicNames()
                    .get()
                    .forEach((name, desc) -> {
                        int partitions = desc.partitions().size();
                        int replicationFactor = desc.partitions().getFirst().replicas().size();
                        long messageCount = calculateMessageCount(kafkaServer, name);
                        int totalInSyncReplicas = desc.partitions().stream()
                                .mapToInt(p -> p.isr().size())
                                .sum();

                        topics.add(new TopicInfo(name, partitions, replicationFactor, messageCount, String.valueOf(totalInSyncReplicas)));
                    });
            return topics;
        } catch (Exception e) {
            Platform.runLater(() -> showAlert(Alert.AlertType.ERROR, "Erro", "Não foi possível buscar os detalhes dos tópicos."));
            return Collections.emptyList();
        }
    }

    private long calculateMessageCount(String kafkaServer, String topic) {
        try (KafkaConsumer<Object, Object> consumer =
                     KafkaConnectionPool.getConsumer(kafkaServer, "kafka-manager-temp-group")) {

            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            return partitions.stream()
                    .mapToLong(p -> endOffsets.get(p) - beginningOffsets.get(p))
                    .sum();
        } catch (Exception e) {
            return 0;
        }
    }

    private boolean sendMessage(String kafkaServer, String topic, String key, String value, Map<String, String> headers) {
        try {
            if (!isConnectionHealthy(kafkaServer)) {
                showAlert(Alert.AlertType.ERROR, "Conexão Inativa", "Reconectando ao servidor...");
                KafkaConnectionPool.getAdminClient(kafkaServer).close(); // Força renovação
            }
            KafkaProducer<Object, Object> producer = KafkaConnectionPool.getProducer(kafkaServer);
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, value);

            headers.forEach((k, v) ->
                    record.headers().add(k, v.getBytes(StandardCharsets.UTF_8))
            );

            producer.send(record).get(5, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private List<String> fetchTopics(String kafkaServer) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        try (Admin admin = Admin.create(props)) {
            return new ArrayList<>(admin.listTopics().names().get(10, TimeUnit.SECONDS));
        } catch (Exception e) {
            showAlert(Alert.AlertType.ERROR, "Erro ao buscar tópicos", "Não foi possível listar os tópicos do servidor.");
            return Collections.emptyList();
        }
    }

    private void showAlert(Alert.AlertType type, String title, String message) {
        Alert alert = new Alert(type);
        alert.setTitle(title);
        alert.setContentText(message);
        alert.showAndWait();
    }

    private boolean isConnectionHealthy(String bootstrapServers) {
        try {
            Admin admin = KafkaConnectionPool.getAdminClient(bootstrapServers);
            admin.describeCluster().nodes().get(5, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void updateConnectionStatus(String kafkaServer, Label statusLabel) {
        CompletableFuture.runAsync(() -> {
            boolean isHealthy = isConnectionHealthy(kafkaServer);
            Platform.runLater(() -> {
                statusLabel.setText(isHealthy ? "✔ Conectado" : "⚠ Conexão Instável");
                statusLabel.setStyle(isHealthy ?
                        "-fx-text-fill: green;" :
                        "-fx-text-fill: orange;");
            });
        });
    }

    private void saveConfigToFile(ProducerConfig config) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<ProducerConfig> configs = loadAllConfigs();
            configs.add(config);

            mapper.writeValue(new File(CONFIG_FILE), configs);
        } catch (IOException ex) {
            showAlert(Alert.AlertType.ERROR, "Erro", "Falha ao salvar configuração");
        }
    }

    private List<ProducerConfig> loadAllConfigs() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            File file = new File(CONFIG_FILE);

            if (file.exists()) {
                return mapper.readValue(file,
                        new TypeReference<>() {
                        });
            }
        } catch (IOException ex) {
            // Arquivo corrompido ou inexistente
        }
        return new ArrayList<>();
    }

    private void initializeProducerConfigs(ComboBox<ProducerConfig> configCombo) {
        List<ProducerConfig> configs = loadAllConfigs();
        configCombo.getItems().addAll(configs);
    }

    public static void main(String[] args) {
        launch(args);
    }
}
