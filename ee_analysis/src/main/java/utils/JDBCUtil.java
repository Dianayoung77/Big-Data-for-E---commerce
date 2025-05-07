package utils;

import java.sql.*;

/**
 * JDBC工具类
 * 提供数据库连接和资源释放功能
 * 适用于电商数据分析项目
 */
public class JDBCUtil {
    // 定义JDBC连接器实例化所需要的固定参数
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    // 修改URL以连接到电商分析数据库
    private static final String MYSQL_URL = "jdbc:mysql://192.168.10.11:3306/ecommerce_analytics?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "000000";

    /**
     * 实例化JDBC连接器对象
     * @return 数据库连接对象
     */
    public static Connection getConnection() {
        try {
            // 加载MySQL驱动
            Class.forName(MYSQL_DRIVER_CLASS);
            // 建立数据库连接
            return DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (ClassNotFoundException e) {
            System.err.println("MySQL驱动加载失败: " + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("数据库连接失败: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 释放数据库连接资源
     * @param connection 数据库连接
     * @param statement SQL语句对象
     * @param resultSet 结果集
     */
    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            // 关闭结果集
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            // 关闭语句对象
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            // 关闭连接
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println("关闭数据库资源失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
