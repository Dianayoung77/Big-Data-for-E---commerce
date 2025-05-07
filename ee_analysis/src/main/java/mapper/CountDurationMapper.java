package mapper;

import kv.key.ComDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import kv.key.ProductDimension;
import kv.value.CountDurationValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 电商数据分析Mapper
 * 从HBase表中读取订单数据，输出用户-日期维度和商品-日期维度的统计值
 */
public class CountDurationMapper extends TableMapper<ComDimension, CountDurationValue> {
    private ComDimension comDimension = new ComDimension();
    private CountDurationValue outputValue = new CountDurationValue();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取rowkey: regionCode_userId_orderTime_productName_flag_price
        String rowKey = Bytes.toString(key.get());
        String[] splits = rowKey.split("_");

        // 如果不是主数据，跳过（flag不为1）
        if(!splits[4].equals("1")) return;

        // 提取数据
        String userId = splits[1];
        String orderTimeString = splits[2]; // 格式: yyyyMMddHHmmss
        String productName = splits[3];
        String price = splits[5];

        // 从HBase行数据中提取其他字段
        String userName = Bytes.toString(value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("userName")));
        String quantity = Bytes.toString(value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("quantity")));
        String totalPrice = Bytes.toString(value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("totalPrice")));
        String orderTime = Bytes.toString(value.getValue(Bytes.toBytes("f1"), Bytes.toBytes("orderTime")));

        // 解析日期
        String year = orderTimeString.substring(0, 4);//
        String month = orderTimeString.substring(4, 6);//
        String day = orderTimeString.substring(6, 8);

        // 组装DateDimension - 按日和按月
        DateDimension dayDimension = new DateDimension(year, month, day);
        DateDimension monthDimension = DateDimension.createMonthDimension(year, month);

        // 组装ContactDimension - 用户信息
        ContactDimension contactDimension = new ContactDimension(userId, userName);

        // 组装ProductDimension - 商品信息
        ProductDimension productDimension = new ProductDimension(productName, price);

        // 准备输出值
        // 订单数为1，商品数量为quantity，金额为totalPrice
        outputValue.setOrderCount("1");
        outputValue.setTotalQuantity(quantity);
        outputValue.setTotalAmount(totalPrice);

        // 1. 分析用户每天购买情况 - 按用户和日期维度
        comDimension.setContactDimension(contactDimension);
        comDimension.setDateDimension(dayDimension);
        comDimension.setProductDimension(new ProductDimension()); // 不关注具体商品
        context.write(comDimension, outputValue);

        // 2. 分析用户每月购买情况 - 按用户和月份维度
        comDimension.setDateDimension(monthDimension);
        context.write(comDimension, outputValue);

        // 3. 分析商品销量情况 - 按商品和日期维度
        comDimension.setContactDimension(new ContactDimension()); // 不关注具体用户
        comDimension.setProductDimension(productDimension);

        // 按日统计商品销量
        comDimension.setDateDimension(dayDimension);
        context.write(comDimension, outputValue);

        // 按月统计商品销量
        comDimension.setDateDimension(monthDimension);
        context.write(comDimension, outputValue);
    }
}
