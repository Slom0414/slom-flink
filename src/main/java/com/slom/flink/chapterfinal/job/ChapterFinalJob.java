package com.slom.flink.chapterfinal.job;

import com.slom.flink.chapterfinal.agg.ChannelAgg;
import com.slom.flink.chapterfinal.delta.ChannelDelta;
import com.slom.flink.chapterfinal.event.OrderEvent;
import com.slom.flink.chapterfinal.function.ChannelAggFlatMap;
import com.slom.flink.chapterfinal.function.OrderDeltaFlatMap;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Objects;

public class ChapterFinalJob {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String hostname = parameterTool.getRequired("mysql.hostname");
        String port = parameterTool.get("mysql.port", "3306");
        String username = parameterTool.getRequired("mysql.username");
        String password = parameterTool.getRequired("mysql.password");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql(
                "CREATE TABLE cold_order_cdc ("
                        + "  id BIGINT,"
                        + "  channel STRING,"
                        + "  amount DECIMAL(18,2),"
                        + "  status STRING,"
                        + "  create_time TIMESTAMP(3),"
                        + "  finish_time TIMESTAMP(3),"
                        + "  update_time TIMESTAMP(3),"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'mysql-cdc',"
                        + "  'hostname' = '" + hostname + "',"
                        + "  'port' = '" + port + "',"
                        + "  'username' = '" + username + "',"
                        + "  'password' = '" + password + "',"
                        + "  'database-name' = 'flink_demo',"
                        + "  'table-name' = 'cold_order'"
                        + ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE hot_order_cdc ("
                        + "  id BIGINT,"
                        + "  channel STRING,"
                        + "  amount DECIMAL(18,2),"
                        + "  status STRING,"
                        + "  create_time TIMESTAMP(3),"
                        + "  finish_time TIMESTAMP(3),"
                        + "  update_time TIMESTAMP(3),"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'mysql-cdc',"
                        + "  'hostname' = '" + hostname + "',"
                        + "  'port' = '" + port + "',"
                        + "  'username' = '" + username + "',"
                        + "  'password' = '" + password + "',"
                        + "  'database-name' = 'flink_demo',"
                        + "  'table-name' = 'hot_order'"
                        + ")"
        );

        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW merged_order_view AS "
                        + "SELECT 'hot' AS src, id, channel, amount, status, create_time, finish_time, update_time "
                        + "FROM hot_order_cdc "
                        + "UNION ALL "
                        + "SELECT 'cold' AS src, id, channel, amount, status, create_time, finish_time, update_time "
                        + "FROM cold_order_cdc"
        );

        Table mergedTable = tableEnv.sqlQuery(
                "SELECT src, id, channel, amount, status, create_time, finish_time, update_time "
                        + "FROM merged_order_view"
        );

        DataStream<Row> rowStream = tableEnv.toChangelogStream(
                mergedTable,
                Schema.newBuilder()
                        .column("src", "STRING")
                        .column("id", "BIGINT")
                        .column("channel", "STRING")
                        .column("amount", "DECIMAL(18,2)")
                        .column("status", "STRING")
                        .column("create_time", "TIMESTAMP(3)")
                        .column("finish_time", "TIMESTAMP(3)")
                        .column("update_time", "TIMESTAMP(3)")
                        .build()
        );

        DataStream<OrderEvent> eventStream = rowStream
                .map(row -> {
                    if (row.getKind() == org.apache.flink.types.RowKind.UPDATE_BEFORE) {
                        return null;
                    }

                    OrderEvent event = new OrderEvent();
                    event.setSrc((String) row.getField("src"));

                    switch (row.getKind()) {
                        case INSERT:
                            event.setOpType("INSERT");
                            break;
                        case UPDATE_AFTER:
                            event.setOpType("UPDATE");
                            break;
                        case DELETE:
                            event.setOpType("DELETE");
                            break;
                        default:
                            return null;
                    }

                    event.setId((Long) row.getField("id"));
                    event.setChannel((String) row.getField("channel"));
                    event.setAmount((BigDecimal) row.getField("amount"));
                    event.setStatus((String) row.getField("status"));
                    event.setCreateTime((LocalDateTime) row.getField("create_time"));
                    event.setFinishTime((LocalDateTime) row.getField("finish_time"));
                    event.setUpdateTime((LocalDateTime) row.getField("update_time"));

                    return event;
                })
                .filter(Objects::nonNull);

        DataStream<ChannelDelta> deltaStream = eventStream
                .keyBy(OrderEvent::getId)
                .flatMap(new OrderDeltaFlatMap());

        DataStream<ChannelAgg> aggStream = deltaStream
                .keyBy(ChannelDelta::getChannel)
                .flatMap(new ChannelAggFlatMap());

        eventStream.print("event");
        deltaStream.print("delta");
        aggStream.print("agg");

        env.execute("chapter-final-job");
    }
}