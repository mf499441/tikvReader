package com.lblj.view.utils;

import com.alibaba.fastjson.JSONObject;
import javafx.scene.control.Alert;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.catalog.Catalog;
import org.tikv.common.meta.TiDBInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Maintainer Daniel.Ma
 * @Email danielma@zhuanxinbaoxian.com
 * @CreateDate 2023/9/14
 * @Version 1.0
 * @Comment
 */
public class TikvUtils {
    public static HashMap<String, ArrayList<String>> getTables(String pdAdress){
        HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
        try{
            TiConfiguration tiConfiguration = TiConfiguration.createDefault(pdAdress);
            TiSession session = TiSession.create(tiConfiguration);
            Catalog catalog = session.getCatalog();
            List<TiDBInfo> tiDBInfos = catalog.listDatabases();
            for (TiDBInfo tiDBInfo : tiDBInfos) {
                ArrayList<String> list = new ArrayList<String>();

                List<TiTableInfo> tableInfos = catalog.listTables(tiDBInfo);
                for (TiTableInfo tb : tableInfos) {
                    list.add(tb.getName());

                }
                map.put(tiDBInfo.getName(),list);
            }
            try {
                session.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }catch (Exception e){
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("异常");
            alert.setHeaderText("TiPD链接异常");
            alert.setContentText(e.getMessage());
            alert.showAndWait();
        }
        return map;
    }



    public static ArrayList<JSONObject> getDataList(String pd, String db, String tb, int i) {
        System.out.println("TikvUtils.getDataList");
        ArrayList<JSONObject> list = new ArrayList<JSONObject>();
        TiConfiguration tiConfiguration = TiConfiguration.createDefault(pd);
        TiSession session = TiSession.create(tiConfiguration);
        TiTableInfo table = session.getCatalog().getTable(db, tb);
        KVClient kvClient = session.createKVClient();

        ArrayList<Pair<ByteString, ByteString>> pairs = new ArrayList<>();
        for (int j = 0; j < 10240; j++) {
            byte[] encode = TidbRowKeyUtil.encode(table.getId(), j);
            ByteString byteKey = ByteString.copyFrom(encode);
            ByteString bytes = kvClient.get(byteKey, session.getTimestamp().getVersion());
            pairs.add(Pair.create(byteKey,bytes));
            if (bytes.size()!=0) {
                Kvrpcpb.KvPair.Builder builder = Kvrpcpb.KvPair.newBuilder();
                builder.setKey(byteKey);
                builder.setValue(bytes);
                Kvrpcpb.KvPair build = builder.build();
                JSONObject dataJSON = TidbRowKeyUtil.getDataJSON(build, table.getColumns(), table);
                dataJSON.put("value_key",j);
                list.add(dataJSON);
                if (list.size()>=i) {
                    kvClient.close();
                    try {
                        session.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return list;
                }
            }
        }
        kvClient.close();
        try {
            session.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;

    }

    public static ArrayList<JSONObject> getSearcheData(String pd, String db, String tb, Long i) {
        ArrayList<JSONObject> list = new ArrayList<JSONObject>();
        TiConfiguration tiConfiguration = TiConfiguration.createDefault(pd);
        TiSession session = TiSession.create(tiConfiguration);
        TiTableInfo table = session.getCatalog().getTable(db, tb);
        KVClient kvClient = session.createKVClient();
        JSONObject dataJSON = TidbRowKeyUtil.searchByKey(session, kvClient, table, i);
        if (dataJSON==null) {
            dataJSON=new JSONObject();
        }
        dataJSON.put("value_key",i);
        list.add(dataJSON);
        kvClient.close();
        try {
            session.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }
}
