package com.atguigu.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        // 从文件中读取数据
//        String inputPath = "D:\\IDEA WORKSPACE\\FlinkTutorial\\src\\main\\resources\\hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用parameter tool工具从程序启动参数中提取配置项
//
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");

        // 从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream("192.168.137.1", 7777);


        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }
}
