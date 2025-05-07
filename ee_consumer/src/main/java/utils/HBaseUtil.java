package utils;

// 此类是Hbase操作的工具类，创建命名空间

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

import static java.lang.System.currentTimeMillis;

public class HBaseUtil {
//
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);// 获取连接
        Admin admin = connection.getAdmin();// 获取admin对象

        boolean result = admin.tableExists(TableName.valueOf(tableName));// 判断表是否存在
        admin.close();// 关闭admin对象
        connection.close();
        return result; // 修正返回结果，使用实际查询结果
    }

    // 初始化命名空间
    public static void initNamespace(Configuration conf, String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // 建表
        NamespaceDescriptor hd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CREATE_TIME", String.valueOf(currentTimeMillis()))
                .addConfiguration("AUTHOR", "lcx").build();

        admin.createNamespace(hd);
        admin.close();
        connection.close();
    }

    // 预制region个数
    public static void createTable(Configuration conf, String tableName, int regions, String... columnFamily)
            throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);// 获取连接
        Admin admin = connection.getAdmin();// 获取admin对象
        if (admin.tableExists(TableName.valueOf(tableName))) return; // 修正逻辑，表存在则返回
        /* 创建表描述符并配置列族 */
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : columnFamily) {// 遍历列族
            htd.addFamily(new HColumnDescriptor(cf));// 添加列族
        }
        // 高级操作
        htd.addCoprocessor("hbase.CalleeWriteObserver");// 添加协处理器
        admin.createTable(htd,genSplitKeys(regions));//

        admin.close();
        connection.close();
    }
//代码解释
//该方法生成HBase预分区键：
// 1. 创建两位数格式化的字符串数组（如"00|","01|"）
// 2. 通过TreeSet进行字节数组排序
// 3. 将有序结果存入二维字节数组返回，用于创建表时的region划分。
    public static byte[][] genSplitKeys(int regions) {
        // 定义一个主键，数组
        String[] Keys = new String[regions];
        // 数据要进行评估，region个数不会超过两位数01-99，分区键的格式为两位数字代表的字符串
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {//
            Keys[i] = df.format(i) + "|";// 格式化字符串
        }
        byte[][] splitKeys = new byte[regions][];//二维字节数组
        // 保证分区键有序
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);//TreeSet有序
        for (int i = 0; i < regions; i++) {// 遍历
            treeSet.add(Bytes.toBytes(Keys[i]));//
        }
        // 比较方法,迭代器
        Iterator<byte[]> splitkeysIterator = treeSet.iterator();
        int index = 0;
        while (splitkeysIterator.hasNext()) {
            byte[] b = splitkeysIterator.next();
            splitKeys[index++] = b;
        }
        return splitKeys;
    }

    // 设计rowkey 定长，散列，反转，加盐，哈希,ascii比大小
    // 适应订单数据格式：用户ID，订单时间，商品名，标记，价格
    public static String genRowKey(String regionCode, String userId, String orderTime, String productName, String flag, String price) {
        StringBuilder sb = new StringBuilder();//可变字符串
        // 从订单时间提取年月
        String ym = orderTime.substring(0, 6);//截取数据
        sb.append(regionCode + "_")
                .append(userId + "_")
                .append(orderTime + "_")
                .append(productName + "_")
                .append(flag + "_")
                .append(price);//拼接
        return sb.toString();//返回字符串
    }

    // 生成区域码
    // 适应订单数据：用户ID和订单时间
    public static String genRegionCode(String userId, String orderTime, int regions) {
        int len = userId.length();
        // 获取用户ID的最后4位
        String lastPart = len >= 4 ? userId.substring(len - 4) : userId;
        // 提取订单时间的年月
        String ym = orderTime.substring(0, 6);

        // 尝试解析为整数并异或操作
        Integer x;
        try {
            x = Integer.valueOf(lastPart) ^ Integer.valueOf(ym);//
        } catch (NumberFormatException e) {
            // 如果解析失败（非数字字符），使用hashCode
            x = lastPart.hashCode() ^ Integer.valueOf(ym);
        }

        // 离散操作，保证散列分布
        int y = x.hashCode();
        // 生成分区号（确保为正数）
        int regionCode = Math.abs(y % regions);
        // 格式化分区号
        DecimalFormat df = new DecimalFormat("00");//预分区策略
        return df.format(regionCode);
    }

    public static String genProductRegionCode(String productName, String orderTime, int regions) {
        // 商品名可能含有特殊字符，使用hashCode更安全
        int productHash = productName.hashCode();

        // 提取订单时间的年月
        String ym = orderTime.substring(0, 6);

        // 结合商品哈希值和年月进行计算
        int combined = productHash ^ Integer.valueOf(ym);

        // 确保为正数并取模
        int regionCode = Math.abs(combined % regions);

        // 格式化为两位数字
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }



}
