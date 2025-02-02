package com.vinicius.kafkamanager;

import javafx.scene.control.TableColumn;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.input.KeyCode;

import java.util.ArrayList;
import java.util.List;

public class AutoCommitTextFieldTableCell<S> extends TextFieldTableCell<S, String> {
    private TextField textField;

    @Override
    public void startEdit() {
        super.startEdit();

        if (textField == null) {
            textField = getTextField();
        }

        // Commit na perda de foco
        textField.focusedProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal) {
                commitEdit(textField.getText());
            }
        });

        // Commit ao pressionar Tab
        textField.setOnKeyPressed(event -> {
            if (event.getCode() == KeyCode.TAB) {
                commitEdit(textField.getText());
                TableColumn<S, ?> nextColumn = getNextColumn();
                if (nextColumn != null) {
                    getTableView().edit(getIndex(), nextColumn);
                }
            }
        });
    }

    private TextField getTextField() {
        return (TextField) getGraphic();
    }

    private TableColumn<S, ?> getNextColumn() {
        List<TableColumn<S, ?>> columns = new ArrayList<>(getTableView().getColumns());
        int currentIndex = columns.indexOf(getTableColumn());
        return currentIndex < columns.size() - 1 ? columns.get(currentIndex + 1) : null;
    }
}