package reducer;

import kv.key.ComDimension;
import kv.value.CountDurationValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 电商数据分析Reducer
 * 聚合来自Mapper的统计结果，生成最终分析报告
 */
public class CountDurationReducer extends Reducer<ComDimension, CountDurationValue, ComDimension, CountDurationValue> {
    private CountDurationValue outputValue = new CountDurationValue();//

    @Override
    protected void reduce(ComDimension key, Iterable<CountDurationValue> values, Context context)//
            throws IOException, InterruptedException {
        // 初始化统计值
        int orderCount = 0;// 订单数
        int totalQuantity = 0;
        double totalAmount = 0.0;

        // 累加所有值
        for (CountDurationValue value : values) {
            orderCount += Integer.parseInt(value.getOrderCount());// 订单数
            totalQuantity += Integer.parseInt(value.getTotalQuantity());
            totalAmount += Double.parseDouble(value.getTotalAmount());
        }

        // 设置输出值
        outputValue.setOrderCount(String.valueOf(orderCount));// 订单数
        outputValue.setTotalQuantity(String.valueOf(totalQuantity));
        outputValue.setTotalAmount(String.valueOf(totalAmount));

        // 输出结果
        context.write(key, outputValue);// key是ComDimension，value是CountDurationValue
    }
}
