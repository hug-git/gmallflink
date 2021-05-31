package com.hug;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


public class Flink03_DataStreamWithMySchema {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2020")
                .tableList("gmall2020.base_trademark")
                .startupOptions(StartupOptions.initial())
//                .startupOptions(StartupOptions.earliest())
//                .startupOptions(StartupOptions.latest())
//                .startupOptions(StartupOptions.specificOffset("/../..", 5))
//                .startupOptions(StartupOptions.timestamp(157L))
                .deserializer(new MyDeserializationSchema())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute();
    }

    public static class MyDeserializationSchema implements DebeziumDeserializationSchema<String> {

        /**
         * {
         *     "data":"{"id":11,"tm_name":"sa"}",
         *     "db":"",
         *     "tableName":"",
         *     "op":"c u d",
         *     "ts":""
         * }
         */
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            // 获取主题信息，提取数据库和表名
            String topic = sourceRecord.topic();
            String[] fields = topic.split("\\.");
            String db = fields[1];
            String tableName = fields[2];

            // 获取Value信息，提取数据本身
            Struct value = (Struct) sourceRecord.value();
            Struct after = value.getStruct("after");
            JSONObject jsonObject = new JSONObject();
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                jsonObject.put(field.name(), o);
            }

            // 获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            // 创建结果JSON
            JSONObject result = new JSONObject();
            result.put("database", db);
            result.put("tableName", tableName);
            result.put("data", jsonObject);
            result.put("op", operation);

            // 输出数据
            collector.collect(result.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
