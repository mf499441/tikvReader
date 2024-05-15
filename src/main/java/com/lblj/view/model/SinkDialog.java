package com.lblj.view.model;

import javafx.scene.control.*;
import javafx.scene.layout.HBox;

/**
 * @Maintainer 蜡笔老舅
 * @CreateDate 2024/2/4
 * @Version 1.0
 * @Comment
 */
public class SinkDialog {
    private String databaseName;
    private String tableName;
    private TextInputDialog dialog;
    private TextArea area;
    public SinkDialog(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        TextArea area = getArea();
        this.area= area;
        this.dialog= initDiaLog(databaseName,tableName,area);
    }

    private TextArea getArea() {
        TextArea area = new TextArea(); // 创建一个多行输入框
        area.setMaxHeight(500); // 设置多行输入框的最大高度
        //area.setMaxWidth(300); // 设置多行输入框的最大宽度
        area.setPrefSize(590, 500); // 设置多行输入框的推荐宽高
        area.setEditable(true); // 设置多行输入框能否编辑
//        area.setPromptText("请输入"); // 设置多行输入框的提示语
        area.setWrapText(true); // 设置多行输入框是否支持自动换行。true表示支持，false表示不支持。
        area.setPrefColumnCount(11); // 设置多行输入框的推荐列数
        area.setPrefRowCount(3); // 设置多行输入框的推荐行数
        return area;
    }

    public TextInputDialog initDiaLog(String databaseName, String tableName, TextArea area){
        TextInputDialog td = new TextInputDialog("写入tikv");

        td.setHeaderText("输入 "+databaseName+"."+tableName+" 数据(请注意 row_id 为 数据Key 值)：");
        ButtonType One = new ButtonType("获取样例数据");
        ButtonType Two = new ButtonType("写入tikv");
        ButtonType Cancel = new ButtonType("取消", ButtonBar.ButtonData.CANCEL_CLOSE);

        td.getDialogPane().getButtonTypes().setAll(One,Two,Cancel);
        HBox hbox = new HBox(); // 创建一个水平箱子
        hbox.setPrefSize(600, 600); // 设置水平箱子的推荐宽高
        Label label = new Label(""); // 创建一个标签
        hbox.getChildren().addAll(label, area); // 给水平箱子添加一个多行输入框

        td.getDialogPane().setContent(hbox);
        return td;
    }

    public TextInputDialog getDialog() {
        return dialog;
    }
    public String getHboxString(){
        return this.area.getText();
    }
    public void setHboxString(String s){
         this.area.setText(s);
    }
}
