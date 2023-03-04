package com.itcj.FlinkSql;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TimeStampDataSource implements SourceFunction<String> {
    private volatile boolean isRunning = true;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        long initialTimestamp = System.currentTimeMillis();
        long startTime = initialTimestamp;

        while (isRunning) {
            long currentTime = System.currentTimeMillis();
            long eventTime = currentTime - initialTimestamp;

            // 格式化时间戳
            String eventTimeString = String.format("%d,%d", eventTime, currentTime);

            // 输出带有时间戳的数据
            sourceContext.collectWithTimestamp(eventTimeString, eventTime);
            sourceContext.emitWatermark(new Watermark(eventTime));

            // 控制数据生成速度，每100毫秒生成一条数据
            Thread.sleep(100);

            // 生成1000条数据后退出
            if (eventTime > 1000) {
                break;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
