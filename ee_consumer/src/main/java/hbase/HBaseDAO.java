package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectionInstance;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * 生成hbase表
 * 数据样式：2022064074,杨敏,商品C,2,79.99,159.98,2024-07-10 13:44:46
 * rowkey样式： regioncode_userId_orderTime_productName_flag_price
 *
 */
public class HBaseDAO {
    private int regions;
    private String namespace;
    private String tableName;
    public static final Configuration conf;
    private HTable table;
    private Connection connection;
    // HBase的连接对象
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    private List<Put> cacheList = new ArrayList<Put>();

    static {
        conf = HBaseConfiguration.create();
        String zookeeperQuorum = PropertiesUtil.getProperty("hbase.zookeeper.quorum");
        conf.set("hbase.zookeeper.quorum", "v1,v2,v3");// zookeeper地址
    }

    // 构造器，构造基本HBaseDAO类，采用解耦方式完成
    // 结构化的方式的好处是不用牵一发而动全身，名字等配置放在配置文件里
    public HBaseDAO() {// 解耦
        try {
            regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.callog.regions"));// 获取分区数
            namespace = PropertiesUtil.getProperty("hbase.callog.namespace");// 获取命名空间
            tableName = PropertiesUtil.getProperty("hbase.callog.tablename");// 获取表名
            connection=ConnectionFactory.createConnection();// 获取连接
            table= (HTable) connection.getTable(TableName.valueOf(tableName));// 获取表

            if (!HBaseUtil.isExistTable(conf, tableName)) {
                HBaseUtil.initNamespace(conf, namespace);// 如果不存在就创建
                HBaseUtil.createTable(conf, tableName, regions, "f1", "f2");//如果没有就创建
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * 数据样式：2022064074,杨敏,商品C,2,79.99,159.98,2024-07-10 13:44:46
     * rowkey样式：regionCode_userId_orderTime_productName_flag_price
     * HBase表的列：userId, userName, productName, quantity, price, totalPrice, orderTime
     * @param ori 输入的订单数据
     */
    public void put(String ori) {
        try {




            if (cacheList.size() == 0) {
                connection = ConnectionInstance.getConnection(conf);
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
            }

            String[] splitOri = ori.split(",");
//            if (splitOri.length != 7) {
//                throw new IllegalArgumentException("输入数据字段数量不正确，应为7个字段");
//            }
            String userId = splitOri[0];
            String userName = splitOri[1];
            String productName = splitOri[2];
            String quantity = splitOri[3];
            String price = splitOri[4];
            String totalPrice = splitOri[5];
            String orderTime = splitOri[6];

            // 格式化订单时间
            String orderTimeReplace = sdf2.format(sdf1.parse(orderTime));

            // 生成区域码 - 使用格式化后的订单时间
            String regionCode = HBaseUtil.genRegionCode(userId, orderTimeReplace, regions);

            String orderTimeTs = String.valueOf(sdf1.parse(orderTime).getTime());

            // 生成rowkey
            String rowkey = HBaseUtil.genRowKey(regionCode, userId, orderTimeReplace, productName, "1", price);

            // 向表中插入该条数据
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("userId"), Bytes.toBytes(userId));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("userName"), Bytes.toBytes(userName));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("productName"), Bytes.toBytes(productName));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("quantity"), Bytes.toBytes(quantity));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("price"), Bytes.toBytes(price));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("totalPrice"), Bytes.toBytes(totalPrice));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("orderTime"), Bytes.toBytes(orderTime));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("orderTimeTs"), Bytes.toBytes(orderTimeTs));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));

            cacheList.add(put);

            if (cacheList.size() >= 20) {
                table.put(cacheList);
                table.close();
                cacheList.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
