package utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;
public class ConnectionInstance {
    private static Connection conn;
    public static synchronized Connection getConnection(Configuration conf) {//
        try {
            if(conn == null || conn.isClosed()){
                conn = ConnectionFactory.createConnection(conf);//创建连接
            }
        } catch (IOException var2) {
            var2.printStackTrace();
        }
        return conn;
    }
}//开启hbase的连接

/**
 * 获取HBase数据库连接的静态同步方法（单例模式）
 *
 * 本方法通过双重检查锁机制实现线程安全的连接获取，当连接不存在或已关闭时，
 * 使用配置对象创建新的HBase连接。注意本实现未完整处理所有异常情况
 *
 * @param conf HBase配置对象，包含集群地址、端口等连接参数
 * @return Connection 返回全局唯一的HBase连接实例
 */