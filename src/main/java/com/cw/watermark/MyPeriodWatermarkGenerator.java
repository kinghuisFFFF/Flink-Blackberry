package com.cw.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class MyPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

    // 乱序等待时间
    private long delayTs;
    // 用来保存 当前为止 最大的事件时间
    private long maxTs;

    public MyPeriodWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来，都会调用一次： 用来提取最大的事件时间，保存下来
     *
     * @param event
     * @param eventTimestamp 提取到的数据的 事件时间
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        System.out.println("调用onEvent方法，获取目前为止的最大时间戳=" + maxTs);
    }

    /**
     * 周期性调用： 发射 watermark
     *我们在 onPeriodicEmit()里调用 output.emitWatermark()，就可以发出水位线了；这个方法
     * 由系统框架周期性地调用，默认 200ms 一次。
     * 如果想修改默认周期时间，可以通过下面方法修改。例如：修改为 400ms
     * env.getConfig().setAutoWatermarkInterval(400L);
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发射水位线，默认 200ms 调用一次
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用onPeriodicEmit方法，生成watermark=" + (maxTs - delayTs - 1));
    }
}
