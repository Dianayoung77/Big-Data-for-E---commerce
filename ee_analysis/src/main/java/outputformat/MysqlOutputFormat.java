package outputformat;

import converter.DimensionConverterImpl;
import kv.key.ComDimension;
import kv.value.CountDurationValue;
import utils.JDBCInstance;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.JDBCUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MySQL输出格式类
 * 负责将MapReduce分析结果写入MySQL数据库
 * 支持以下三个业务表：
 * 1. tb_user_daily_stats - 用户每天购买商品数量和金额统计
 * 2. tb_user_monthly_stats - 用户每月购买频率和累计消费金额统计
 * 3. tb_product_stats - 商品日销量、月销量统计
 */
public class MysqlOutputFormat extends OutputFormat<ComDimension, CountDurationValue> {
    private OutputCommitter committer = null;

    @Override
    //创建记录写入器，定义数据如何写入目标系统（如MySQL
    public RecordWriter<ComDimension, CountDurationValue> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        // 初始化JDBC连接器对象，添加连接重试逻辑，建立数据库连接
        Connection conn = null;

        // 尝试获取数据库连接，最多尝试3次
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                conn = JDBCInstance.getInstance();
                if (conn != null) {
                    // 成功获取连接
                    conn.setAutoCommit(false);
                    break;
                }
            } catch (SQLException e) {
                if (i == maxRetries - 1) {
                    // 最后一次尝试仍然失败，抛出异常
                    throw new IOException("无法获取数据库连接，已重试" + maxRetries + "次: " + e.getMessage(), e);
                }
                try {
                    // 等待一段时间后重试
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("连接数据库时被中断", ie);
                }
            }
        }

        // 如果连接仍然为null，抛出异常
        if (conn == null) {
            throw new IOException("无法获取数据库连接，请检查数据库配置和网络连接");
        }

        return new MysqlRecordWriter(conn);//返回一个MysqlRecordWriter对象，写入器
    }

    @Override
    //在Job提交前校验输出配置是否有效
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // 增加数据库连接检查
        try {
            Connection testConn = JDBCInstance.getInstance();
            if (testConn != null) {//如果连接成功
                // 连接成功，关闭测试连接
                testConn.close();//
            } else {
                throw new IOException("数据库连接测试失败，无法获取连接");
            }
        } catch (SQLException e) {
            throw new IOException("数据库连接测试失败: " + e.getMessage(), e);
        }
    }

    @Override
    //用于管理作业的提交和任务的状态协调。
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);// 获取输出路径
            Path outputPath = name == null ? null : new Path(name);// 转换为Path对象
            committer = new FileOutputCommitter(outputPath, context);// 创建FileOutputCommitter对象
        }
        return committer;
    }

    /**
     * MySQL记录写入器
     * 负责将MapReduce结果写入三个不同的MySQL表
     */
    static class MysqlRecordWriter extends RecordWriter<ComDimension, CountDurationValue> {
        // 维度转换器，用于获取维度ID
        private DimensionConverterImpl dci = new DimensionConverterImpl();// 维度转换器

        // 数据库连接和预编译语句
        private Connection conn = null;

        // 用户每日统计表预编译语句
        private PreparedStatement userDailyPstmt = null;
        private String userDailySQL = null;
        private int userDailyCount = 0;

        // 用户每月统计表预编译语句
        private PreparedStatement userMonthlyPstmt = null;
        private String userMonthlySQL = null;
        private int userMonthlyCount = 0;

        // 商品销量统计表预编译语句
        private PreparedStatement productPstmt = null;
        private String productSQL = null;
        private int productCount = 0;

        // 批处理大小
        private final int BATCH_SIZE = 500;

        /**
         * 构造函数
         * @param conn 数据库连接
         */
        public MysqlRecordWriter(Connection conn) {
            this.conn = conn;
        }

        /**
         * 检查并尝试重新连接数据库
         * @return 是否成功连接
         */
        private boolean checkConnection() {
            if (conn == null) {
                try {
                    conn = JDBCInstance.getInstance();
                    if (conn != null) {
                        conn.setAutoCommit(false);
                        return true;
                    }
                } catch (SQLException e) {
                    System.err.println("重新连接数据库失败: " + e.getMessage());
                    return false;
                }
            }

            try {
                // 检查连接是否仍然有效
                if (conn.isClosed() || !conn.isValid(5)) {
                    try {
                        // 重新获取连接
                        conn = JDBCInstance.getInstance();
                        if (conn != null) {
                            conn.setAutoCommit(false);
                            return true;
                        }
                    } catch (SQLException e) {
                        System.err.println("重新连接数据库失败: " + e.getMessage());
                        return false;
                    }
                }
                return true;
            } catch (SQLException e) {
                System.err.println("检查数据库连接失败: " + e.getMessage());
                return false;
            }
        }

        @Override
        public void write(ComDimension key, CountDurationValue value) throws IOException, InterruptedException {
            // 确保数据库连接有效
            if (!checkConnection()) {
                // 无法连接数据库时记录错误信息，但继续处理下一条记录
                System.err.println("警告: 无法连接数据库，跳过当前记录");
                return;
            }

            try {
                // 获取维度ID，负责将维度对象转换为数据库中的主键 id。
                int idDateDimension = dci.getDimensionID(key.getDateDimension());
                int idContactDimension = dci.getDimensionID(key.getContactDimension());
                int idProductDimension = dci.getDimensionID(key.getProductDimension());

                // 解析日期维度，判断是按日还是按月
                String day = key.getDateDimension().getDay();
                boolean isDailyStats = !"-1".equals(day) && !"00".equals(day);//

                // 获取值
                int orderCount = Integer.parseInt(value.getOrderCount());
                int totalQuantity = Integer.parseInt(value.getTotalQuantity());
                double totalAmount = Double.parseDouble(value.getTotalAmount());

                // 检查是否有效的用户维度
                boolean isValidUserDimension = idContactDimension > 0 &&
                        !"".equals(key.getContactDimension().getUserId());

                // 检查是否有效的商品维度
                boolean isValidProductDimension = idProductDimension > 0 &&
                        !"".equals(key.getProductDimension().getProductName());

                // 1. 处理用户每日统计
                if (isValidUserDimension && isDailyStats) {
                    // 复合主键: 日期维度ID_用户维度ID
                    String idDateContact = idDateDimension + "_" + idContactDimension;

                    // 初始化SQL语句
                    if (userDailySQL == null) {
                        userDailySQL = "INSERT INTO `tb_user_daily_stats` " +
                                "(`id_date_contact`, `id_date_dimension`, `id_contact`, " +
                                "`order_count`, `total_quantity`, `total_amount`) " +
                                "VALUES (?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "`order_count` = VALUES(`order_count`), " +
                                "`total_quantity` = VALUES(`total_quantity`), " +
                                "`total_amount` = VALUES(`total_amount`);";
                    }

                    // 初始化预编译语句
                    if (userDailyPstmt == null) {
                        userDailyPstmt = conn.prepareStatement(userDailySQL);
                    }//PreparedStatement 是 Java 中用于执行预编译 SQL 语句的对象。
                    // 它允许你在执行 SQL 语句之前设置参数值

                    // 设置参数。在构建好 SQL 语句后，需要设置具体的参数值。
                    // 这些参数值包括各个维度的 id 以及统计值（如订单数、总数量、总金额等）
                    int i = 0;
                    userDailyPstmt.setString(++i, idDateContact);// 主键
                    userDailyPstmt.setInt(++i, idDateDimension);// 日期维度ID
                    userDailyPstmt.setInt(++i, idContactDimension);// 联系人维度ID
                    userDailyPstmt.setInt(++i, orderCount);// 订单数量
                    userDailyPstmt.setInt(++i, totalQuantity);// 总数量
                    userDailyPstmt.setDouble(++i, totalAmount);// 总金额
//
                    // 添加到批处理
                    userDailyPstmt.addBatch();// 添加到批处理
                    userDailyCount++;// 计数器加1

                    // 达到批处理大小时执行批处理
                    if (userDailyCount >= BATCH_SIZE) {
                        userDailyPstmt.executeBatch();
                        conn.commit();
                        userDailyCount = 0;
                        userDailyPstmt.clearBatch();
                    }
                }

                // 2. 处理用户每月统计
                if (isValidUserDimension && !isDailyStats) {
                    // 复合主键: 日期维度ID_用户维度ID (这里日期维度是月级别)
                    String idDateContact = idDateDimension + "_" + idContactDimension;

                    // 初始化SQL语句
                    if (userMonthlySQL == null) {
                        userMonthlySQL = "INSERT INTO `tb_user_monthly_stats` " +
                                "(`id_month_contact`, `id_date_dimension`, `id_contact`, " +
                                "`purchase_frequency`, `total_amount`) " +
                                "VALUES (?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "`purchase_frequency` = VALUES(`purchase_frequency`), " +
                                "`total_amount` = VALUES(`total_amount`);";
                    }

                    // 初始化预编译语句
                    if (userMonthlyPstmt == null) {
                        userMonthlyPstmt = conn.prepareStatement(userMonthlySQL);
                    }

                    // 设置参数
                    int i = 0;
                    userMonthlyPstmt.setString(++i, idDateContact);
                    userMonthlyPstmt.setInt(++i, idDateDimension);
                    userMonthlyPstmt.setInt(++i, idContactDimension);
                    userMonthlyPstmt.setInt(++i, orderCount); // 购买频率
                    userMonthlyPstmt.setDouble(++i, totalAmount);

                    // 添加到批处理
                    userMonthlyPstmt.addBatch();
                    userMonthlyCount++;

                    // 达到批处理大小时执行批处理
                    if (userMonthlyCount >= BATCH_SIZE) {
                        userMonthlyPstmt.executeBatch();
                        conn.commit();
                        userMonthlyCount = 0;
                        userMonthlyPstmt.clearBatch();
                    }
                }

                // 3. 处理商品销量统计
                if (isValidProductDimension) {
                    // 复合主键: 日期维度ID_商品维度ID
                    String idDateProduct = idDateDimension + "_" + idProductDimension;

                    // 初始化SQL语句
                    if (productSQL == null) {
                        productSQL = "INSERT INTO `tb_product_stats` " +
                                "(`id_date_product`, `id_date_dimension`, `id_product`, " +
                                "`sales_count`, `sales_amount`) " +
                                "VALUES (?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "`sales_count` = VALUES(`sales_count`), " +
                                "`sales_amount` = VALUES(`sales_amount`);";
                    }

                    // 初始化预编译语句
                    if (productPstmt == null) {
                        productPstmt = conn.prepareStatement(productSQL);
                    }

                    // 设置参数
                    int i = 0;
                    productPstmt.setString(++i, idDateProduct);// 复合主键
                    productPstmt.setInt(++i, idDateDimension);
                    productPstmt.setInt(++i, idProductDimension);
                    productPstmt.setInt(++i, totalQuantity); // 销量
                    productPstmt.setDouble(++i, totalAmount);

                    // 添加到批处理
                    productPstmt.addBatch();
                    productCount++;

                    // 达到批处理大小时执行批处理
                    if (productCount >= BATCH_SIZE) {
                        productPstmt.executeBatch();
                        conn.commit();
                        productCount = 0;
                        productPstmt.clearBatch();
                    }
                }

            } catch (SQLException e) {
                System.err.println("处理记录时发生SQL错误: " + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                System.err.println("处理记录时发生未预期错误: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                // 确保连接有效
                if (conn != null && !conn.isClosed()) {
                    // 1. 提交剩余的用户每日统计批次
                    if (userDailyPstmt != null && userDailyCount > 0) {
                        try {
                            userDailyPstmt.executeBatch();
                            userDailyCount = 0;
                        } catch (SQLException e) {
                            System.err.println("执行用户每日统计批处理时出错: " + e.getMessage());
                        }
                    }

                    // 2. 提交剩余的用户每月统计批次
                    if (userMonthlyPstmt != null && userMonthlyCount > 0) {
                        try {
                            userMonthlyPstmt.executeBatch();
                            userMonthlyCount = 0;
                        } catch (SQLException e) {
                            System.err.println("执行用户每月统计批处理时出错: " + e.getMessage());
                        }
                    }

                    // 3. 提交剩余的商品销量统计批次
                    if (productPstmt != null && productCount > 0) {
                        try {
                            productPstmt.executeBatch();
                            productCount = 0;
                        } catch (SQLException e) {
                            System.err.println("执行商品销量统计批处理时出错: " + e.getMessage());
                        }
                    }

                    // 4. 提交事务
                    try {
                        conn.commit();
                    } catch (SQLException e) {
                        System.err.println("提交事务时出错: " + e.getMessage());
                    }
                }
            } catch (SQLException e) {
                System.err.println("关闭资源时检查连接状态出错: " + e.getMessage());
            } finally {
                // 关闭所有预编译语句
                if (userDailyPstmt != null) {
                    try {
                        userDailyPstmt.close();
                    } catch (SQLException e) {
                        // 忽略关闭时的错误
                    }
                }

                if (userMonthlyPstmt != null) {
                    try {
                        userMonthlyPstmt.close();
                    } catch (SQLException e) {
                        // 忽略关闭时的错误
                    }
                }

                if (productPstmt != null) {
                    try {
                        productPstmt.close();
                    } catch (SQLException e) {
                        // 忽略关闭时的错误
                    }
                }

                // 关闭数据库连接
                JDBCUtil.close(conn, null, null);
            }
        }
    }
}
