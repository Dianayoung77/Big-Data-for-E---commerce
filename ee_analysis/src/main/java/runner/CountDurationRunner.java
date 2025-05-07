package runner;

import kv.key.ComDimension;
import kv.value.CountDurationValue;
import mapper.CountDurationMapper;
import outputformat.MysqlOutputFormat;
import reducer.CountDurationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

/**
 * 电商数据分析MapReduce作业运行器
 * 负责配置和启动MapReduce作业，从HBase读取数据，分析电商交易数据
 */
//CountDurationRunner.java 的主要作用是：
//配置和启动 MapReduce 作业：通过 run 方法配置作业的各个部分（如 Mapper、Reducer、输入输出等），并启动作业。
//配置 HBase 输入源：通过 initHBaseInputConfig 方法配置从 HBase 表中读取订单数据。
//配置 Reducer 和输出格式：通过 initReducerOutputConfig 方法配置 Reducer 类、输出键值类型以及输出格式（将结果写入 MySQL 数据库

//CountDurationRunner 类：实现了 Tool 接口，
// 这是 Hadoop 提供的一个接口，用于简化命令行参数的处理。
public class CountDurationRunner implements Tool {
    private Configuration conf = null;// 用于存储作业的配置信息

    @Override
    //设置配置信息
    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    //获取配置信息。
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        // 获取配置
        Configuration conf = this.getConf();

        // 设置作业名称，更清晰地表示业务含义
        Job job = Job.getInstance(conf, "E-commerce Data Analysis");
        job.setJarByClass(CountDurationRunner.class);

        // 配置HBase输入
        initHBaseInputConfig(job);

        // 配置Reducer和输出
        initReducerOutputConfig(job);

        // 提交作业并等待完成
        return job.waitForCompletion(true) ? 0 : 1;// 返回作业执行结果，0成功，1失败
    }

    /**
     * 初始化HBase输入配置
     * 从HBase表中读取订单数据
     * @param job MapReduce作业
     */



    //initHBaseInputConfig 方法：配置 HBase 输入源。
    //创建 HBase 连接：使用 ConnectionFactory.createConnection 方法创建 HBase 连接。
    //检查表是否存在：通过 admin.tableExists 方法检查指定的 HBase 表是否存在。
    //配置 Scan 对象：设置 Scan 对象的缓存、批处理和列族等参数，优化读取性能。


    private void initHBaseInputConfig(Job job) {
        Connection connection = null;
        Admin admin = null;
        try {
            // 使用正确的HBase表名
            String tableName = "tsxy:callog";

            // 创建HBase连接
            connection = ConnectionFactory.createConnection(job.getConfiguration());
            admin = connection.getAdmin();

            // 检查表是否存在
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                throw new RuntimeException("无法找到HBase表: " + tableName);
            }

            // 配置Scan对象
            Scan scan = new Scan();

            // 性能优化设置
            scan.setCaching(500);         // 设置每次从服务器端读取的行数
            scan.setBatch(100);           // 设置每次从服务器端读取的列数
            scan.setCacheBlocks(false);   // 禁用块缓存以优化MapReduce作业的性能

            // 设置要扫描的列族（如mapper中使用的列族f1）
            scan.addFamily(org.apache.hadoop.hbase.util.Bytes.toBytes("f1"));

            // 初始化TableMapper，输出类型为ComDimension和CountDurationValue
            // 与CountDurationMapper的输出类型匹配
            TableMapReduceUtil.initTableMapperJob(
                    tableName,                  // 输入表
                    scan,                       // 扫描配置
                    CountDurationMapper.class,  // Mapper类
                    ComDimension.class,         // Mapper输出键类型
                    CountDurationValue.class,   // Mapper输出值类型
                    job,
                    true                        // 添加依赖Jar
            );

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            try {
                if (admin != null) {
                    admin.close();
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 初始化Reducer和输出配置
     * 设置Reducer类和MySQL输出格式
     * @param job MapReduce作业
     */

    //配置 Reducer 和输出格式
    private void initReducerOutputConfig(Job job) {
        // 设置Reducer类
        job.setReducerClass(CountDurationReducer.class);

        // 设置输出键值类型
        job.setOutputKeyClass(ComDimension.class);
        job.setOutputValueClass(CountDurationValue.class);

        // 设置输出格式为MySQL输出
        job.setOutputFormatClass(MysqlOutputFormat.class);

        // 设置Reducer数量，根据数据量和表进行合理设置
        job.setNumReduceTasks(3); // 为三个不同的表设置三个Reducer
    }

    /**
     * 主函数，程序入口
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        try {
            // 运行MapReduce作业
            //创建 CountDurationRunner 实例：并通过 ToolRunner.run 方法运行作业
            int status = ToolRunner.run(new CountDurationRunner(), args);//
            System.exit(status);//根据作业的执行状态退出程序（0 表示成功，非 0 表示失败）
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
