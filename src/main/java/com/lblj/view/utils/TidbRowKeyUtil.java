package com.lblj.view.utils;

/**
 * @Maintainer Daniel.Ma
 * @Email danielma@zhuanxinbaoxian.com
 * @CreateDate 2023/3/17
 * @Version 1.0
 * @Comment 通过 tikv 查询 tidb 数据 的解码
 */

import com.alibaba.fastjson.JSONObject;
import org.tikv.common.TiSession;
import org.tikv.common.codec.Codec.IntegerCodec;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.key.Key;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.tikv.common.codec.TableCodec.decodeObjects;
import static org.tikv.common.key.RowKey.toRowKey;

public class TidbRowKeyUtil extends Key implements Serializable {
    private static final byte[] REC_PREFIX_SEP = new byte[]{95, 114};
    private final long tableId;
    private final long handle;
    private final boolean maxHandleFlag;

    private TidbRowKeyUtil(long tableId, long handle) {
        super(encode(tableId, handle));
        this.tableId = tableId;
        this.handle = handle;
        this.maxHandleFlag = false;
    }


    public static RowKey createMin(long tableId) {
        return toRowKey(tableId, Long.MIN_VALUE);
    }



    // 解密rowKey
    public static RowKey decode(byte[] value) {
        CodecDataInput cdi = new CodecDataInput(value);
        byte b = cdi.readByte();
        long tableId = IntegerCodec.readLong(cdi);
        cdi.readByte();
        cdi.readByte();
        long handle = IntegerCodec.readLong(cdi);
        return toRowKey(tableId, handle);
    }

    //通过tableId handle 加密
    public static byte[] encode(long tableId, long handle) {
        CodecDataOutput cdo = new CodecDataOutput();
        encodePrefix(cdo, tableId);
        encodeRow(cdo,handle);
        return cdo.toBytes();
    }



    //生产加密key的前缀（table id 加密）
    private static void encodePrefix(CodecDataOutput cdo, long tableId) {
        // 增加 /t table_id 标识
        cdo.write(TBL_PREFIX);
        cdo.writeLong(tableId ^ Long.MIN_VALUE);
        // 写入一个 /r row_id 标识
        cdo.write(REC_PREFIX_SEP);
    }

    //生产加密key的前缀（row id 加密）
    private static void encodeRow(CodecDataOutput cdo, long rowId) {
        // 对row_id 进行编码
        cdo.writeLong(rowId ^ Long.MIN_VALUE);
    }

    public static JSONObject searchByKey(TiSession session, KVClient kvClient, TiTableInfo tableBInfo, Long key) {
        byte[] encode = TidbRowKeyUtil.encode(tableBInfo.getId(), key);
        ByteString byteKey = ByteString.copyFrom(encode);
        ByteString bytes = kvClient.get(byteKey, session.getTimestamp().getVersion());
        if (bytes.size()!=0) {
            Kvrpcpb.KvPair.Builder builder = Kvrpcpb.KvPair.newBuilder();
            builder.setKey(byteKey);
            builder.setValue(bytes);
            Kvrpcpb.KvPair build = builder.build();
            JSONObject dataJSON = TidbRowKeyUtil.getDataJSON(build, tableBInfo.getColumns(), tableBInfo);
            return dataJSON;
        }
        return null;
    }


    public String toString() {
        return Long.toString(this.handle);
    }


    // 通过解析到的tidb Value 解析为JSON 数据
    public static JSONObject getDataJSON(Kvrpcpb.KvPair record, List<TiColumnInfo> columns, TiTableInfo tiTableInfo) {
        JSONObject jsonObject = new JSONObject();
        Object[] tikvValues =
                decodeObjects(
                        record.getValue().toByteArray(),
                        RowKey.decode(record.getKey().toByteArray()).getHandle(),
                        tiTableInfo);
        List<Object> objects = Arrays.asList(tikvValues);
        for (int i = 0; i < tikvValues.length; i++) {
            String type = columns.get(i).getType().getName();
            jsonObject.put(columns.get(i).getName(),getDataObject(objects.get(i),type));
        }
        return jsonObject;
    }

    // tidb timestamp和time 格式的数据，进行特别处理
    public static Object getDataObject(Object o, String type) {

        if (o!=null) {
            switch(type.toLowerCase()) {
                case "timestamp": case "datetime@asia/shanghai":
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(o);
                case "time":
                    return new SimpleDateFormat("HH:mm:ss").format(new Date(Long.parseLong(o.toString())));
                default:
                    return o;
            }
        }
        return o;
    }
}