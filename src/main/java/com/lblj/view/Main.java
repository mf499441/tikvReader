package com.lblj.view;


import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.net.URL;
import java.util.prefs.Preferences;

public class Main extends Application {

    @Override
    public void start(Stage primaryStage) throws Exception{
        Preferences prefs = Preferences.userNodeForPackage(this.getClass());
        GlobalSettings.loadSettings(prefs);
        URL resource = getClass().getResource("/sample.fxml");
        Parent root = FXMLLoader.load(resource);



        primaryStage.setTitle("TIKV读取工具");
        primaryStage.setScene(new Scene(root, 580, 400));
        primaryStage.setResizable(true); // 禁止缩放窗口
        primaryStage.show();
    }


    public static void main(String[] args) {
        launch(args);
    }
}
