package com.lblj.view.contorl; /**
 * Sample Skeleton for 'sample.fxml' Controller Class
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.lblj.view.GlobalSettings;
import com.lblj.view.model.TableValue;
import com.lblj.view.utils.TikvUtils;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;

import java.net.URL;
import java.util.*;
import java.util.function.UnaryOperator;

public class PleaseProvideControllerClassName {


    @FXML // ResourceBundle that was given to the FXMLLoader
    private ResourceBundle resources;

    @FXML // URL location of the FXML file that was given to the FXMLLoader
    private URL location;

    @FXML // fx:id="tx"
    private TextArea tx; // Value injected by FXMLLoader

    @FXML // fx:id="bu"
    private Button bu; // Value injected by FXMLLoader

    @FXML // fx:id="pdadress"
    private TextArea pdadress; // Value injected by FXMLLoader

    @FXML // fx:id="save"
    private Button save; // Value injected by FXMLLoader

    @FXML // fx:id="connect"
    private Button connect; // Value injected by FXMLLoader


    @FXML // fx:id="tree"
    private TreeView<String> tree; // Value injected by FXMLLoader

    @FXML
    private TextArea num;

    @FXML // fx:id="table"
    private TableView<TableValue> table; // Value injected by FXMLLoader

    @FXML
    private TableColumn<TableValue, String> tableKeys;

    @FXML
    private TableColumn<TableValue, String> tableValues;




    

    @FXML // fx:id="cho"
    private ComboBox<String> cho; // Value injected by FXMLLoader

    @FXML
    void connectPD(ActionEvent event) {
        // 创建根节点
        connect.setText("获取中");

        Thread thread = new Thread(() -> {
            TreeItem<String> item1 = new TreeItem<String>("tidb");
            String text = pdadress.getText();
            Platform.runLater(() -> {
                if (text != null && !text.equals("")) {
                    HashMap<String, ArrayList<String>> tables = TikvUtils.getTables(text);
                    tables.forEach((k,v)->{
                        TreeItem<String> db = new TreeItem<>(k);
                        for (String s : v) {
                            db.getChildren().add(new TreeItem<>(s));
                        }
                        item1.getChildren().add(db);
                    });
                }
                tree.setRoot(item1);

            });
        });

        thread.start();
         connect.setText("连接");
    }


    @FXML
    void onAction(ActionEvent event) {
        MultipleSelectionModel<TreeItem<String>> selectionModel = tree.getSelectionModel();
        ObservableList<TreeItem<String>> selectedItems = selectionModel.getSelectedItems();
        tx.setVisible(false);
        TreeItem<String> item = selectedItems.get(0);
        String text = num.getText();

        if (item.getValue()==null||item.getValue().equals("")||text==null|| text.equals("")) {
        Alert alert = new Alert(Alert.AlertType.WARNING);
        alert.setTitle("异常");
        alert.setHeaderText("空值异常");
        alert.setContentText("消费数据量不为空或者未选择表名");
        alert.showAndWait();
        }else {
            Thread thread = new Thread(() -> {
                Platform.runLater(() -> {

                    if (cho.getSelectionModel().getSelectedIndex() == 0) {
                        int i = Integer.parseInt(text);
                        ArrayList<JSONObject> dataList = TikvUtils.getDataList(pdadress.getText(), item.getParent().getValue(), item.getValue(), i);
                        ArrayList<TableValue> values = truToTableValue(dataList);
                        ObservableList<TableValue> data = FXCollections.observableArrayList(values);
                        table.setItems(data);
                        table.setVisible(true);

                        table.getSelectionModel().selectedItemProperty().addListener((observableValue, oldValue, newValue) -> {
                            if (newValue != null) {
                                tx.setVisible(true);
                                String formatStr = JSON.toJSONString(JSONObject.parseObject(newValue.getValueValue())
                                        , SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
                                        SerializerFeature.WriteDateUseDateFormat);
                                tx.setText(formatStr);
                            }
                        });
                    } else if (cho.getSelectionModel().getSelectedIndex() == 1) {
                        Long i = Long.parseLong(text);
                        ArrayList<JSONObject> dataList = TikvUtils.getSearcheData(pdadress.getText(), item.getParent().getValue(), item.getValue(), i);
                        ArrayList<TableValue> values = truToTableValue(dataList);
                        ObservableList<TableValue> data = FXCollections.observableArrayList(values);
                        table.setItems(data);
                        table.setVisible(true);

                        table.getSelectionModel().selectedItemProperty().addListener((observableValue, oldValue, newValue) -> {
                            if (newValue != null) {
                                tx.setVisible(true);
                                String formatStr = JSON.toJSONString(JSONObject.parseObject(newValue.getValueValue())
                                        , SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
                                        SerializerFeature.WriteDateUseDateFormat);
                                tx.setText(formatStr);
                            }
                        });
                    }
                });
            });
            thread.start();
        }


    }

    private ArrayList<TableValue> truToTableValue(ArrayList<JSONObject> dataList) {
        ArrayList<TableValue> va = new ArrayList<>();
        dataList.forEach(json->{
            va.add(new TableValue(json.remove("value_key").toString(),json.toJSONString()));
        });
        return va;
    }

    @FXML
    void saveAdress(ActionEvent event) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("提示");
        alert.setHeaderText("保存地址");
        alert.setContentText("是否保存PD Adress?");
        alert.showAndWait();
        if (alert.getResult().getText().equals("确定")&& pdadress.getText()!=null) {
            GlobalSettings.setSetting("pd_adress",pdadress.getText());
        }
    }

    @FXML // This method is called by the FXMLLoader when initialization is complete
    void initialize() {
        if (GlobalSettings.getSetting("pd_adress")!=null) {
            pdadress.setText(GlobalSettings.getSetting("pd_adress"));
        }
        assert cho != null : "fx:id=\"cho\" was not injected: check your FXML file 'sample.fxml'.";
        List<String> list1 = Arrays.asList("消费数据", "指定key消费");
        cho.getItems().addAll(list1);
        cho.getSelectionModel().select(0);


        tableKeys.setCellValueFactory(new PropertyValueFactory<TableValue, String>("keyValue"));
        tableValues.setCellValueFactory(new PropertyValueFactory<TableValue, String>("valueValue"));
        table.setVisible(false);
        tx.setEditable(false);
        tx.setVisible(false);

        TextFormatter textFormatter = new TextFormatter(new UnaryOperator<TextFormatter.Change>() {
            @Override
            public TextFormatter.Change apply(TextFormatter.Change change) {
                if (change.getText().matches("\\d*")) {
                    return change;
                }
                return null;
            }
        });
        num.setTextFormatter(textFormatter);

        assert tx != null : "fx:id=\"tx\" was not injected: check your FXML file 'sample.fxml'.";
        assert bu != null : "fx:id=\"bu\" was not injected: check your FXML file 'sample.fxml'.";
        assert tree != null : "fx:id=\"tree\" was not injected: check your FXML file 'sample.fxml'.";
        assert pdadress != null : "fx:id=\"pdadress\" was not injected: check your FXML file 'sample.fxml'.";
        assert save != null : "fx:id=\"save\" was not injected: check your FXML file 'sample.fxml'.";
        assert connect != null : "fx:id=\"connect\" was not injected: check your FXML file 'sample.fxml'.";

    }
}
