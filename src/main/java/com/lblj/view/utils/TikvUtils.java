package com.lblj.view.utils;

import com.alibaba.fastjson.JSONObject;
import com.pingcap.com.google.common.base.Preconditions;
import com.pingcap.com.google.common.collect.ImmutableList;
import javafx.scene.control.Alert;
import org.tikv.common.BytePairWrapper;
import org.tikv.common.Snapshot;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.catalog.Catalog;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.codec.MetaCodec;
import org.tikv.common.codec.TableCodec;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiDBInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.tikv.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;
import org.tikv.txn.TwoPhaseCommitter;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static com.lblj.view.utils.TidbRowKeyUtil.getDataJSON;

/**
 * @Maintainer 蜡笔老舅
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
            Snapshot snapshot = new Snapshot(session.getTimestamp(), session);
            List<Pair<ByteString, ByteString>> fields = MetaCodec.hashGetFields(MetaCodec.KEY_DBs, snapshot);
            Iterator var3 = fields.iterator();
            ObjectMapper mapper = new ObjectMapper();
            while(var3.hasNext()) {
                Pair<ByteString, ByteString> pair = (Pair)var3.next();
                TiDBInfo tiDBInfo = mapper.readValue(((ByteString) pair.second).toStringUtf8(), TiDBInfo.class);
                String dbName = tiDBInfo.getName();
                ByteString dbKey = MetaCodec.encodeDatabaseID(tiDBInfo.getId());
                List<Pair<ByteString, ByteString>> pairs = MetaCodec.hashGetFields(dbKey, snapshot);
                Iterator<Pair<ByteString, ByteString>> var6 = pairs.iterator();
                ArrayList<String> list = new ArrayList<String>();
                while (var6.hasNext()) {
                    Pair<ByteString, ByteString> tablePair = (Pair)var6.next();
                    if (KeyUtils.hasPrefix((ByteString)tablePair.first, ByteString.copyFromUtf8(MetaCodec.KEY_TABLE))) {
                        try {
                            TiTableInfo tableInfo = mapper.readValue(((ByteString) tablePair.second).toStringUtf8(), TiTableInfo.class);
                            list.add(tableInfo.getName());
                        } catch (TiClientInternalException var9) {

                        }
                    }
                }
                map.put(dbName,list);
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





    public static ArrayList<JSONObject> getDataList(TiSession session, String db, String tb, int i) {
        ArrayList<JSONObject> list = new ArrayList<JSONObject>();
        TiTableInfo table = session.getCatalog().getTable(db, tb);
        long tableID = table.getId();
        Coprocessor.KeyRange keyRange = getTableKeyRange(
                tableID,1,0);
        KVClient kvClient = session.createKVClient();
        long startTs = session.getTimestamp().getVersion();
        final List<Kvrpcpb.KvPair> segment =
                kvClient.scan(keyRange.getStart(), keyRange.getEnd(), startTs);

        for (final Kvrpcpb.KvPair pair : segment) {
            if (isRecordKey(pair.getKey().toByteArray())) {
                List<TiColumnInfo> columns = table.getColumns();
                JSONObject dataJSON = getDataJSON(pair, columns,table);
                list.add(dataJSON);
                if (list.size()==i) {
                    break;
                }
            }
        }

        kvClient.close();
        return list;

    }

    public static boolean isRecordKey(byte[] key) {
        return key[9] == 95 && key[10] == 114;
    }

    public static Coprocessor.KeyRange getTableKeyRange(long tableId, int num, int idx) {
        Preconditions.checkArgument(idx >= 0 && idx < num, "Illegal value of idx");
        return (Coprocessor.KeyRange)getTableKeyRanges(tableId, num).get(idx);
    }


    public static List<Coprocessor.KeyRange> getTableKeyRanges(long tableId, int num) {
        Preconditions.checkArgument(num > 0, "Illegal value of num");
        if (num == 1) {
            return ImmutableList.of(getTableKeyRange(tableId));
        } else {
            long delta = BigInteger.valueOf(9223372036854775807L).subtract(BigInteger.valueOf(-9223372036854775807L)).divide(BigInteger.valueOf((long)num)).longValueExact();
            ImmutableList.Builder<Coprocessor.KeyRange> builder = ImmutableList.builder();

            for(int i = 0; i < num; ++i) {
                RowKey startKey = i == 0 ? RowKey.createMin(tableId) : RowKey.toRowKey(tableId, -9223372036854775808L + delta * (long)i);
                RowKey endKey = i == num - 1 ? RowKey.createBeyondMax(tableId) : RowKey.toRowKey(tableId, -9223372036854775808L + delta * (long)(i + 1));
                builder.add(KeyRangeUtils.makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
            }

            return builder.build();
        }
    }

    public static Coprocessor.KeyRange getTableKeyRange(long tableId) {
        return KeyRangeUtils.makeCoprocRange(RowKey.createMin(tableId).toByteString(), RowKey.createBeyondMax(tableId).toByteString());
    }


    public static ArrayList<JSONObject> getSearcheData(TiSession session, String db, String tb, Long i) {
        ArrayList<JSONObject> list = new ArrayList<JSONObject>();
        TiTableInfo table = session.getCatalog().getTable(db, tb);
        KVClient kvClient = session.createKVClient();
        JSONObject dataJSON = TidbRowKeyUtil.searchByKey(session, kvClient, table, i);
        if (dataJSON==null) {
            dataJSON=new JSONObject();
        }
        dataJSON.put("row_id",i);
        list.add(dataJSON);
        kvClient.close();

        return list;
    }

    public static ArrayList<String> getDatabases(TiSession session) {
        ArrayList<String> list = new ArrayList<>();
        Snapshot snapshot = new Snapshot(session.getTimestamp(), session);
        List<Pair<ByteString, ByteString>> fields = MetaCodec.hashGetFields(MetaCodec.KEY_DBs, snapshot);
        Iterator var3 = fields.iterator();
        ObjectMapper mapper = new ObjectMapper();
        while (var3.hasNext()) {
            Pair<ByteString, ByteString> pair = (Pair) var3.next();
            TiDBInfo tiDBInfo = null;
            try {
                tiDBInfo = mapper.readValue(((ByteString) pair.second).toStringUtf8(), TiDBInfo.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            String dbName = tiDBInfo.getName();
            list.add(dbName);
        }
        return list;
    }

    public static ArrayList<String> getTablesV2(TiSession session, String dbName) {
        ArrayList<String> talbleList = new ArrayList<>();
        try{
            Catalog catalog = session.getCatalog();
            List<TiTableInfo> tiTableInfos = catalog.listTables(catalog.getDatabase(dbName));
            tiTableInfos.forEach(tiTableInfo -> talbleList.add(tiTableInfo.getName()));
        }catch (Exception e){
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("异常");
            alert.setHeaderText("TiPD链接异常");
            alert.setContentText(e.getMessage());
            alert.showAndWait();
        }finally {
            return talbleList;
        }
    }

    public static ArrayList<String> sinkToTable(TiSession session, List<JSONObject> value,String databaseName,String tableName) {
        ArrayList<String> talbleList = new ArrayList<>();
        try{
            Catalog catalog = session.getCatalog();
            TiTableInfo table = catalog.getTable(databaseName, tableName);
            List<TiColumnInfo> columns = table.getColumns();
            List<BytePairWrapper> finalCachedValues = new ArrayList<>();

            value.forEach(json -> {
                // 首先先check json,将需要的数据留下来，不需要的去掉
                Long row_id = Long.parseLong(json.remove("row_id").toString());
                Object[] objects = jsonToObjects(json,columns);
                RowKey rowKey = RowKey.toRowKey(table.getId(), row_id);
                try {
                    byte[] value2 = TableCodec.encodeRow(
                            table.getColumns(), objects, true, true);
                    BytePairWrapper bytePairWrapper = new BytePairWrapper(
                            rowKey.getBytes(),
                            value2);
                    finalCachedValues.add(bytePairWrapper);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
            KVSet(session, finalCachedValues);

        }catch (Exception e){
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("异常");
            alert.setHeaderText("TiPD链接异常");
            alert.setContentText(e.getMessage());
            alert.showAndWait();
        }finally {
            return talbleList;
        }
    }

    private static Object[] jsonToObjects(JSONObject json,List<TiColumnInfo> columns) {
        Object[] objects = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Object o = json.get(columns.get(i).getName());
            objects[i] = transObject(o, columns.get(i));
        }
        return objects;

    }

    private static Object transObject(Object o, TiColumnInfo tiColumnInfo) {
        // tidb 数据类型和java 类型记录在下面的链接中
        // https://doc.weixin.qq.com/sheet/e3_ADUA2Qa8ABkcj0stk5RTx21yxgrP4?scode=AOEAHAcyAA4BLYlH54ADUA2Qa8ABk&version=4.1.3.6008&platform=win
        String name = tiColumnInfo.getType().getType().name();
        if (o == null) {
            return null;
        }
        switch (name) {
            case "TypeLonglong":
            case "TypeLong":
            case "TypeInt24":
            case "TypeShort":
            case "TypeDuration":
            case "TypeTiny": {
                if (o instanceof Integer) {
                    return Long.parseLong(o.toString());
                }
                if (o instanceof Boolean) {
                    return 0l;
                }
                return (Long) o;
            }
            case "TypeVarchar":
            case "TypeString": {
                return (String) o;
            }
            case "TypeFloat": {
                return (Float) o;
            }
            case "TypeDouble": {
                if (o instanceof BigDecimal || o instanceof Integer) {
                    return Double.parseDouble(o.toString());
                }
                return (Double) o;
            }
            case "TypeDate": {
                return Date.valueOf(o.toString());
            }
            case "TypeDatetime":
            case "TypeTimestamp": {
                return Timestamp.valueOf(o.toString());
            }
            case "TypeMediumBlob":
            case "TypeLongBlob":
            case "TypeBlob": {
                return o.toString().getBytes();
            }
        }
        return null;

    }


    public static void KVSet(TiSession session, @Nonnull List<BytePairWrapper> pairs) {
        Iterator<BytePairWrapper> iterator = pairs.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        TwoPhaseCommitter twoPhaseCommitter = new TwoPhaseCommitter(session, session.getTimestamp().getVersion());
        BytePairWrapper primaryPair = iterator.next();

        try {
            twoPhaseCommitter.prewritePrimaryKey(
                    ConcreteBackOffer.newCustomBackOff(2000),
                    primaryPair.getKey(),
                    primaryPair.getValue());

            if (iterator.hasNext()) {
                twoPhaseCommitter.prewriteSecondaryKeys(
                        primaryPair.getKey(), iterator, 2000);
            }

            twoPhaseCommitter.commitPrimaryKey(
                    ConcreteBackOffer.newCustomBackOff(1000),
                    primaryPair.getKey(),
                    session.getTimestamp().getVersion());

        } catch (Throwable ignored) {
            ignored.printStackTrace();
        } finally {
            try {
                twoPhaseCommitter.close();
            } catch (Exception e) {

                e.printStackTrace();
            }

        }
    }

}
