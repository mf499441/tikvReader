package com.lblj.view;

/**
 * @Maintainer 蜡笔老舅
 * @CreateDate 2023/9/11
 * @Version 1.0
 * @Comment
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.prefs.Preferences;

public class GlobalSettings {
    private static Preferences preferences ;
    private static String pdAdress ="";

    public static void setSetting(String key, String value) {
        preferences.put(key, value);
    }

    public static String getSetting(String key) {
        return preferences.get(key,null);
    }


    public static void loadSettings(Preferences prefs) {
        preferences=prefs;
    }
}
