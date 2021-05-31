package com.hug;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_DataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setAlignmentTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkgmall/ck"));

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
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute();
    }
}
