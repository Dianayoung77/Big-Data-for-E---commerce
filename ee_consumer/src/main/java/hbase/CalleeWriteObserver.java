package hbase;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Optional;

public class CalleeWriteObserver implements RegionObserver, RegionCoprocessor {
    // 注册了一个格式转换器
    // RegionObserver检测，RegionCoprocessor运行
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    // 创建一个协处理环境对象，空参构造器
    private RegionCoprocessorEnvironment env = null;// 环境对象

    // 拿到RegionObserver对象，获取环境，都是重写方法
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        // Extremely important to be sure that the coprocessor is invoked as a RegionObserver
        return Optional.of(this);// 返回当前对象
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {//
        env = (RegionCoprocessorEnvironment) e;// 获取环境
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // nothing to do here
    }

    // 用preput,在数据写入前获取，数据属于后进
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit, final Durability durability)
            throws IOException {

        // 1、获取你想要操作的目标表的名称 - 从配置文件中读取实际表名
        String targetTableName = PropertiesUtil.getProperty("tsxy:callog");

        // 2、获取当前成功Put了数据的表
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();

        // 判断是否是目标表，在拿到数据
        if(!targetTableName.equals(currentTableName)) return;

        // 通过put拿到当前要写入数据的rowkey
        String oriRowKey = Bytes.toString(put.getRow());

        // 根据下划线分拆开一条数据
        String[] splitOriRowKey = oriRowKey.split("_");

        // 验证rowkey格式是否正确
        if (splitOriRowKey.length < 6) {
            // 记录日志或者处理无效数据
            return;
        }

        // 获取标记位，判断是主数据(1)还是反向关联数据(2)
        String oldFlag = splitOriRowKey[4];

        // 如果当前插入的是反向关联数据，则直接返回
        if(oldFlag.equals("2")) return;

        // 获取regions数量
        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.callog.regions"));

        // 重组数据，生成反向关联数据，并进行写入到Hbase表
        String userId = splitOriRowKey[1];           // 用户ID
        String orderTime = splitOriRowKey[2];        // 订单时间
        String productName = splitOriRowKey[3];      // 商品名称
        String flag = "2";                           // 将1换成2作为反向关联的标记
        String price = splitOriRowKey[5];            // 价格

        // 生成region号码，以商品为主键生成
        // 使用专门为商品设计的方法
        String regionCode = HBaseUtil.genProductRegionCode(productName, orderTime, regions);


        // 生成反向关联数据的rowkey (以商品名为主键)
        String productRowKey = HBaseUtil.genRowKey(regionCode, productName, orderTime, userId, flag, price);

        // 对应表写入列簇和列
        Put productPut = new Put(Bytes.toBytes(productRowKey));
        productPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("productName"), Bytes.toBytes(productName));
        productPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("userId"), Bytes.toBytes(userId));
        productPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("orderTime"), Bytes.toBytes(orderTime));
        productPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        productPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("price"), Bytes.toBytes(price));

        long timestamp;//
        try {
            timestamp = sdf.parse(orderTime).getTime();// 时间戳
        } catch (ParseException ex) {
            // 使用当前时间作为备选
            timestamp = System.currentTimeMillis();
            // 也可以考虑记录日志
        }
        productPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("orderTimeTs"), Bytes.toBytes(timestamp));//

        Table table = null;
        try {
            table = e.getEnvironment().getConnection().getTable(TableName.valueOf(targetTableName));
            table.put(productPut);
        } catch (IOException ex) {
            // 处理异常，可能需要记录日志
            throw ex; // 或者根据业务需求决定是否继续传播异常
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException closeEx) {
                    // 处理关闭异常，通常只需记录日志
                }
            }
        }
    }
}
