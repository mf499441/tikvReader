package com.lblj.view.contorl; /**
 * Sample Skeleton for 'sample.fxml' Controller Class
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.lblj.view.GlobalSettings;
import com.lblj.view.model.SinkDialog;
import com.lblj.view.model.TableValue;
import com.lblj.view.utils.TikvUtils;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;
import java.util.function.UnaryOperator;

public class TIKVReaderController {


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

    private  TiSession session ;

    public TIKVReaderController() {
    }


    @FXML
    void connectPD(ActionEvent event) {

        Stage window = (Stage)tx.getScene().getWindow();
        window.setOnCloseRequest(e -> {
            if (this.session!=null) {
                try {
                    session.close();
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        });
        // 创建根节点
        connect.setText("获取中");
        Thread thread = new Thread(() -> {
            TreeItem<String> item1 = new TreeItem<String>("tidb");
            String text = pdadress.getText();
            Platform.runLater(() -> {
                if (text != null && !text.equals("")) {
                    if (this.session!=null) {
                        try {
                            session.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    TiConfiguration tiConfiguration = TiConfiguration.createDefault(text);
                    this.session = TiSession.create(tiConfiguration);
                    ArrayList<String> databases = TikvUtils.getDatabases(session);
                    for (String database : databases) {
                        TreeItem<String> db = new TreeItem<>(database);
                        item1.getChildren().add(db);
                    }
                }
                tree.setRoot(item1);

            });
        });

        MultipleSelectionModel<TreeItem<String>> selectionModel = tree.getSelectionModel();
        tree.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                TreeItem<String> item = selectionModel.getSelectedItems().get(0);
                if (item!=null) {
                    String itemValue = item.getValue();
                    if(event.getClickCount() == 2){
                        if (!itemValue.equals("tidb")&& item.getChildren().size()==0) {
                            ArrayList<String> tableList = TikvUtils.getTablesV2(session, itemValue);
                            tableList.forEach(name->{
                                item.getChildren().add(new TreeItem<>(name));
                            });
                        }
                    }
                }
//                if (event.getButton()== MouseButton.PRIMARY){
//                    System.out.println("单击"); 层数
//                }
                if (event.getButton()== MouseButton.SECONDARY && item.getChildren().size()==0) {
                    ContextMenu cm = new ContextMenu();
                    if (getItemLayer(item,0)==2) {
                        if (tree.getContextMenu()==null) {
                            String itemName = item.getValue();
                            String parentName = item.getParent().getValue();
                            MenuItem item2 = new MenuItem("写入数据");
                            cm.getItems().addAll(item2);
                            tree.setContextMenu(cm);
                            cm.setOnAction(new EventHandler<ActionEvent>() {
                                @Override
                                public void handle(ActionEvent event) {
                                    if (((MenuItem) (event.getTarget())).getText().equals("写入数据")) {
                                        SinkDialog sinkDialog = new SinkDialog(parentName, itemName);
                                        TextInputDialog diaLog = sinkDialog.getDialog();
                                        diaLog.show();
                                        ButtonType buttonType = diaLog.getDialogPane().getButtonTypes().get(0);
                                        ButtonType buttonType1 = diaLog.getDialogPane().getButtonTypes().get(1);

                                        Button node = (Button)diaLog.getDialogPane().lookupButton(buttonType);
                                        node.addEventFilter(ActionEvent.ACTION,eventSen -> {
//                                            System.out.println("请求样例数据");
                                            try{
                                                ArrayList<JSONObject> dataList = TikvUtils.getDataList(session,parentName, itemName,  1);
                                                if (dataList!=null && dataList.size()>0) {
                                                    JSONObject jsonObject = dataList.get(0);
                                                    String s = JSON.toJSONString(JSONObject.parseObject(jsonObject.toJSONString())
                                                            , SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
                                                            SerializerFeature.WriteDateUseDateFormat);
                                                    sinkDialog.setHboxString(s);
                                                }
                                            }catch (Exception e){
                                                Alert alert = new Alert(Alert.AlertType.WARNING);
                                                alert.setTitle("异常");
                                                alert.setHeaderText("空值异常");
                                                alert.setContentText("样例数据请求失败，表中可能没有数据");
                                                alert.showAndWait();
//                                                diaLog.close();
                                            }
                                            eventSen.consume();

                                        });

                                        Button node1 = (Button)diaLog.getDialogPane().lookupButton(buttonType1);
                                        node1.addEventFilter(ActionEvent.ACTION,eventSen -> {
                                            String hboxString = sinkDialog.getHboxString();
                                            if (hboxString!=null) {
                                                JSONObject json = JSONObject.parseObject(hboxString);
                                                ArrayList<JSONObject> list = new ArrayList<>();
                                                list.add(json);
                                                if (json.get("row_id")!=null) {
                                                    try{
                                                        TikvUtils.sinkToTable(session,list,parentName,itemName);
                                                        Alert alert = new Alert(Alert.AlertType.INFORMATION);
                                                        alert.setTitle("完成");
                                                        alert.setHeaderText("写入数据完成");
                                                        alert.setContentText("写入数据完成" );
                                                        alert.showAndWait();


                                                    }catch (Exception e){
                                                        Alert alert = new Alert(Alert.AlertType.WARNING);
                                                        alert.setTitle("异常");
                                                        alert.setHeaderText("写入数据异常");
                                                        alert.setContentText("异常如下：" +e.getMessage());
                                                        alert.showAndWait();
                                                    }
                                                }else {
                                                    Alert alert = new Alert(Alert.AlertType.WARNING);
                                                    alert.setTitle("异常");
                                                    alert.setHeaderText("数据没有row_id 字段");
                                                    alert.setContentText("数据没有row_id");
                                                    alert.showAndWait();
                                                }
                                            }else {
                                                Alert alert = new Alert(Alert.AlertType.WARNING);
                                                alert.setTitle("异常");
                                                alert.setHeaderText("数据为空");
                                                alert.setContentText("数据为空");
                                                alert.showAndWait();
                                            }
                                            eventSen.consume();

                                        });


                                    }
                                }
                            });

                            cm.setOnShowing(new EventHandler<WindowEvent>() {
                                @Override
                                public void handle(WindowEvent event) {

                                }
                            });

                        }
                    }else if (tree.getContextMenu()!=null){
                        tree.setContextMenu(null);
                    }
                }else if (tree.getContextMenu()!=null){
                    tree.setContextMenu(null);
                }
            }

            private int getItemLayer(TreeItem<String> item, Integer i) {
                TreeItem<String> parent = item.getParent();
                if (parent !=null) {
                    i++;
                    if (parent.getValue().equals("tidb")) {
                        return i;
                    }else {
                        return getItemLayer(parent, i);
                    }
                }
                return i;
            }
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
                        ArrayList<JSONObject> dataList =new ArrayList<>();
                        try{
                            dataList = TikvUtils.getDataList(session, item.getParent().getValue(), item.getValue(), i);

                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        ArrayList<TableValue> values=new ArrayList<>();
                        try{
                            values = truToTableValue(dataList);

                        }catch (Exception e){
                            e.printStackTrace();
                        }
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
                        ArrayList<JSONObject> dataList = TikvUtils.getSearcheData(this.session, item.getParent().getValue(), item.getValue(), i);
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
        try{
            dataList.forEach(json->{
                va.add(new TableValue(json.remove("row_id").toString(),json.toJSONString()));
            });

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            return va;

        }
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
