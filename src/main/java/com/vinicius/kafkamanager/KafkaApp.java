package com.vinicius.kafkamanager;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class KafkaApp extends Application {

    // Variável de controle para cancelar a conexão
    private CompletableFuture<Void> connectionFuture;

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Kafka Manager");

        // Ícone do Kafka
        ImageView kafkaIcon = new ImageView(
                new Image(Objects.requireNonNull(getClass().getResource("/kafka-background.png")).toExternalForm())
        );
        kafkaIcon.setFitWidth(100);
        kafkaIcon.setPreserveRatio(true);

        // Campo de texto para Kafka Server
        Label kafkaLabel = new Label("Kafka Host:");
        TextField kafkaField = new TextField();
        kafkaField.setPromptText("Digite o endereço do servidor Kafka (e.g. localhost:9092)");

        // Botões
        Button connectButton = new Button("Conectar");
        Button cancelButton = new Button("Cancelar");
        cancelButton.setId("cancelButton");
        cancelButton.setDisable(true); // Desativado inicialmente

        // Adicionar evento para tecla Enter
        kafkaField.setOnKeyPressed(event -> {
            switch (event.getCode()) {
                case ENTER -> connectButton.fire();
            }
        });

        // Loader
        ProgressIndicator loader = new ProgressIndicator();
        loader.setVisible(false);

        // Ações do botão "Conectar"
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

        // Ações do botão "Cancelar"
        cancelButton.setOnAction(e -> {
            if (connectionFuture != null && !connectionFuture.isDone()) {
                connectionFuture.cancel(true); // Cancela a conexão em andamento
            }
            connectButton.setDisable(false);
            cancelButton.setDisable(true);
            loader.setVisible(false);
            showAlert(Alert.AlertType.INFORMATION, "Cancelado", "Tentativa de conexão cancelada.");
        });

        // Layout
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

    // Função para abrir a lista de tópicos
    private void openTopicListView(String kafkaServer, Stage primaryStage) {
        BorderPane layout = new BorderPane();

        // Menu Superior (Horizontal)
        HBox menu = new HBox(10);
        menu.setPadding(new Insets(10));
        menu.setStyle("-fx-background-color: #f0f0f0;");
        menu.setAlignment(Pos.CENTER_LEFT);

// Botões de Tópicos e Producers
        Button topicsButton = new Button("Tópicos");
        Button producersButton = new Button("Producers");
        Button exitButton = new Button("Sair");
        exitButton.setStyle("-fx-background-color: red; -fx-text-fill: white;");
        exitButton.setOnAction(e -> primaryStage.close());

// Alinhar os botões "Tópicos" e "Producers" à esquerda
        menu.getChildren().addAll(topicsButton, producersButton);

// Alinhar o botão "Sair" à direita
        HBox rightSide = new HBox(exitButton);
        rightSide.setAlignment(Pos.CENTER_RIGHT);
        menu.getChildren().add(rightSide);  // Coloca o "Sair" na parte direita

        // Layout do topo
        layout.setTop(menu);

        // Placeholder inicial
        Label placeholderLabel = new Label("Selecione uma opção no menu.");
        placeholderLabel.setStyle("-fx-font-size: 16; -fx-text-fill: gray;");
        layout.setCenter(placeholderLabel);

        // Ação do botão Tópicos
        topicsButton.setOnAction(e -> showTopicsView(kafkaServer, layout));

        // Ação do botão Producers
        producersButton.setOnAction(e -> showProducersForm(kafkaServer, layout));

        // Rodapé
        Label footerLabel = new Label("Conectado ao host: " + kafkaServer);
        footerLabel.setStyle("-fx-text-fill: black;");

        HBox footer = new HBox(10, footerLabel);
        footer.setPadding(new Insets(10));
        footer.setStyle("-fx-background-color: #ddd;");
        footer.setAlignment(Pos.CENTER_LEFT);

        // Garantir que os elementos fiquem alinhados corretamente
        HBox.setHgrow(footerLabel, Priority.ALWAYS);
        footer.setAlignment(Pos.CENTER_LEFT);

        layout.setBottom(footer);

        // Cena principal
        Scene scene = new Scene(layout, 800, 600);
        primaryStage.setScene(scene);
    }



    // Função para testar a conexão com o Kafka
    private boolean testKafkaConnection(String kafkaServer) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        try (Admin admin = Admin.create(props)) {
            admin.describeCluster().nodes().get(10, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            showAlert(Alert.AlertType.ERROR, "Erro na Conexão", "Detalhes: " + e.getMessage());
            return false;
        }
    }

    // Exibir Tópicos
    // Exibir Tópicos
    private void showTopicsView(String kafkaServer, BorderPane layout) {
        TableView<TopicInfo> table = new TableView<>();

        // Configurar colunas da tabela
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

        // Label de carregamento
        Label loadingLabel = new Label("Carregando dados...");
        loadingLabel.setStyle("-fx-font-size: 14px; -fx-text-fill: gray;");

        // Botão de atualizar
        Button refreshButton = new Button("Atualizar");
        refreshButton.setOnAction(e -> loadTopics(kafkaServer, table, loadingLabel));

        // Layout para o botão de atualizar e a tabela
        VBox vbox = new VBox(10, refreshButton, table);
        vbox.setPadding(new Insets(10));

        // Layout principal
        layout.setCenter(vbox);

        // Carregar os tópicos inicialmente
        loadTopics(kafkaServer, table, loadingLabel);
    }

    // Função para carregar tópicos
    private void loadTopics(String kafkaServer, TableView<TopicInfo> table, Label loadingLabel) {
        table.getItems().clear(); // Limpa a tabela antes de carregar novos dados
        table.setPlaceholder(loadingLabel); // Mostra o label enquanto carrega os dados

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



    // Exibir Formulário de Producers
    private void showProducersForm(String kafkaServer, BorderPane layout) {
        VBox form = new VBox(10);
        form.setPadding(new Insets(10));

        // Combobox para selecionar o tópico
        Label topicLabel = new Label("Selecione o Tópico:");
        ComboBox<String> topicComboBox = new ComboBox<>();
        topicComboBox.setEditable(true); // Permite a digitação para filtro

        // Carregar os tópicos do Kafka
        CompletableFuture.runAsync(() -> {
            List<String> topics = fetchTopics(kafkaServer);
            Platform.runLater(() -> topicComboBox.getItems().addAll(topics));
        });

        topicComboBox.setMaxWidth(Double.MAX_VALUE);  // Ocupa 200% do width

        // Campo de texto para "Chave"
        Label keyLabel = new Label("Chave:");
        TextField keyField = new TextField();
        keyField.setPromptText("Digite a chave (opcional)");
        keyField.setMaxWidth(Double.MAX_VALUE);  // Ocupa 100% do width

        // Campo de texto para "Valor" (JSON)
        Label valueLabel = new Label("Valor:");
        TextArea valueArea = new TextArea();
        valueArea.setPromptText("Digite o valor em JSON");
        valueArea.setMaxWidth(Double.MAX_VALUE);  // Ocupa 100% do width

        // Tabela para os Headers (HashMap<String, String>)
        Label headersLabel = new Label("Headers:");
        TableView<Header> headersTable = new TableView<>();

        // Configurar a tabela de headers para permitir edição
        TableColumn<Header, String> headerKeyColumn = new TableColumn<>("Chave");
        headerKeyColumn.setCellValueFactory(data -> data.getValue().key());
        headerKeyColumn.setCellFactory(TextFieldTableCell.forTableColumn()); // Permitir edição
        headerKeyColumn.setOnEditCommit(event -> {
            Header header = event.getRowValue();
            header.setKey(event.getNewValue()); // Atualiza a chave no modelo
        });

        TableColumn<Header, String> headerValueColumn = new TableColumn<>("Valor");
        headerValueColumn.setCellValueFactory(data -> data.getValue().value());
        headerValueColumn.setCellFactory(TextFieldTableCell.forTableColumn()); // Permitir edição
        headerValueColumn.setOnEditCommit(event -> {
            Header header = event.getRowValue();
            header.setValue(event.getNewValue()); // Atualiza o valor no modelo
        });

        // Permitir que a tabela seja editável
        headersTable.setEditable(true);
        headersTable.getColumns().addAll(headerKeyColumn, headerValueColumn);
        headersTable.setMaxWidth(Double.MAX_VALUE);  // Ocupa 100% do width

        // Botões para adicionar/remover Headers
        HBox headersButtons = new HBox(10);
        Button addHeaderButton = new Button("Adicionar Header");
        Button removeHeaderButton = new Button("Remover Header");

        // Ação do botão "Adicionar Header"
        addHeaderButton.setOnAction(e -> {
            // Adicionar um novo Header com valores vazios
            headersTable.getItems().add(new Header("", ""));
        });

        // Ação do botão "Remover Header"
        removeHeaderButton.setOnAction(e -> {
            // Remover o Header selecionado
            Header selectedHeader = headersTable.getSelectionModel().getSelectedItem();
            if (selectedHeader != null) {
                headersTable.getItems().remove(selectedHeader);
            }
        });

        headersButtons.getChildren().addAll(addHeaderButton, removeHeaderButton);

        // Botão para enviar a mensagem
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

            // Converter headers em HashMap
            Map<String, String> headers = new HashMap<>();
            for (Header header : headersTable.getItems()) {
                headers.put(header.getKey(), header.getValue());
            }

            // Enviar a mensagem
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

        form.getChildren().addAll(
                topicLabel, topicComboBox,
                keyLabel, keyField,
                valueLabel, valueArea,
                headersLabel, headersTable, headersButtons,
                sendButton
        );

        layout.setCenter(form);
    }


    // Função para buscar detalhes dos tópicos
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
                        int replicationFactor = desc.partitions().get(0).replicas().size(); // Fator de replicação assumido igual para todas as partições
                        long messageCount = calculateMessageCount(kafkaServer, name); // Calcula o número de mensagens
                        int totalInSyncReplicas = desc.partitions().stream()
                                .mapToInt(p -> p.isr().size())
                                .sum(); // Soma de réplicas em sincronização

                        topics.add(new TopicInfo(name, partitions, replicationFactor, messageCount, String.valueOf(totalInSyncReplicas)));
                    });
            return topics;
        } catch (Exception e) {
            Platform.runLater(() -> showAlert(Alert.AlertType.ERROR, "Erro", "Não foi possível buscar os detalhes dos tópicos."));
            return Collections.emptyList();
        }
    }

    private long calculateMessageCount(String kafkaServer, String topic) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaServer);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("group.id", "kafka-manager-group");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<org.apache.kafka.common.TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(topic, p.partition()))
                    .toList();

            // Busca os offsets finais
            Map<org.apache.kafka.common.TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            // Busca os offsets iniciais
            Map<org.apache.kafka.common.TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            // Calcula a soma das diferenças (número total de mensagens)
            return partitions.stream()
                    .mapToLong(partition -> endOffsets.get(partition) - beginningOffsets.get(partition))
                    .sum();
        } catch (Exception e) {
            Platform.runLater(() -> showAlert(Alert.AlertType.ERROR, "Erro", "Falha ao calcular o número de mensagens para o tópico: " + topic));
            return 0;
        }
    }

    // Função para enviar mensagem
    private boolean sendMessage(String kafkaServer, String topic, String message, String value, Map<String, String> headers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServer);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, message));
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    // Função para buscar os tópicos do Kafka
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

    // Função para exibir alertas
    private void showAlert(Alert.AlertType type, String title, String message) {
        Alert alert = new Alert(type);
        alert.setTitle(title);
        alert.setContentText(message);
        alert.showAndWait();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
