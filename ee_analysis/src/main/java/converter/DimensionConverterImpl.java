package converter;

import kv.base.BaseDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import kv.key.ProductDimension;
import utils.JDBCInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.JDBCUtil;
import utils.LRUCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 维度转换器实现类
 *
 * 工作流程：
 * 1、根据传入的维度数据，得到该数据对应的在表中的主键id
 * ** 做内存缓存，LRUCache
 * if(缓存中有数据){
 *     直接返回id
 * }else if(缓存中无数据){
 *     则查询MySql
 *     if(MySql中有该条数据){
 *         直接返回id
 *         将本次读取到的id缓存到内存中
 *     }else if(MySql中没有该条数据){
 *         插入该条数据
 *         再次反查该数据，得到id并返回
 *         将本次读取到的id缓存到内存中
 *     }
 * }
 */
public class DimensionConverterImpl implements DimensionConverter {
    // Logger
    private static final Logger logger = LoggerFactory.getLogger(DimensionConverterImpl.class);
    // 对象线程化 用于每个线程管理自己的JDBC连接器
    private ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();
    // 构建内存缓存对象
    private LRUCache<String, Integer> lruCache = new LRUCache<>(3000);

    // 构造器
    public DimensionConverterImpl() {
        // jvm关闭时，释放资源
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> JDBCUtil.close(threadLocalConnection.get(), null, null)));
    }

    @Override
    public int getDimensionID(BaseDimension dimension) {
        // 1、根据传入的维度对象获取对应的主键id，先从LRUCache中获取
        // 时间维度：date_dimension_year_month_day
        // 用户维度：contact_dimension_userId_userName
        // 商品维度：product_dimension_productName
        String cacheKey = genCacheKey(dimension);// 获取缓存keygenCacheKey

        // 尝试获取缓存的id
        if (lruCache.containsKey(cacheKey)) {
            return lruCache.get(cacheKey);
        }

        // 没有得到缓存id，需要执行select操作
        // sqls包含了1组sql语句：查询和插入语句
        String[] sqls = null;
        if (dimension instanceof DateDimension) {
            sqls = getDateDimensionSQL();// 获取时间维度的sql语句
        } else if (dimension instanceof ContactDimension) {
            sqls = getContactDimensionSQL();
        } else if (dimension instanceof ProductDimension) {
            sqls = getProductDimensionSQL();// 获取商品维度的sql语句
        } else {
            throw new RuntimeException("没有匹配到对应维度信息.");
        }

        // 准备对MySQL表进行操作，先查询，有可能再插入
        Connection conn = this.getConnection();// 获取当前线程维护的Connection对象
        int id = -1;// 初始化id为-1
        synchronized (this) {// 同步代码块
            id = execSQL(conn, sqls, dimension);// 执行sql语句
        }
        // 将刚查询到的id加入到缓存中
        lruCache.put(cacheKey, id);
        return id;
    }

    /**
     * 得到当前线程维护的Connection对象
     */
    private Connection getConnection() {
        Connection conn = null;
        try {
            conn = threadLocalConnection.get();
            if (conn == null || conn.isClosed()) {
                conn = JDBCInstance.getInstance();
                threadLocalConnection.set(conn);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 执行SQL语句
     * @param conn      JDBC连接器
     * @param sqls      长度为2，第一个位置为查询语句，第二个位置为插入语句
     * @param dimension 对应维度所保存的数据
     * @return 维度ID
     */
    private int execSQL(Connection conn, String[] sqls, BaseDimension dimension) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            // 1. 查询
            preparedStatement = conn.prepareStatement(sqls[0]);
            setArguments(preparedStatement, dimension);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                int result = resultSet.getInt(1);
                JDBCUtil.close(null, preparedStatement, resultSet);
                return result;
            }
            JDBCUtil.close(null, preparedStatement, resultSet);

            // 2. 插入
            preparedStatement = conn.prepareStatement(sqls[1]);
            setArguments(preparedStatement, dimension);
            preparedStatement.executeUpdate();
            JDBCUtil.close(null, preparedStatement, null);

            // 3. 再次查询
            preparedStatement = conn.prepareStatement(sqls[0]);// 重新获取连接
            setArguments(preparedStatement, dimension);// 重新设置参数
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtil.close(null, preparedStatement, resultSet);
        }
        return -1;
    }

    /**
     * 设置SQL语句的具体参数
     */
    private void setArguments(PreparedStatement preparedStatement, BaseDimension dimension) {
        int i = 0;
        try {
            if (dimension instanceof DateDimension) {
                DateDimension dateDimension = (DateDimension) dimension;
                preparedStatement.setString(++i, dateDimension.getYear());
                preparedStatement.setString(++i, dateDimension.getMonth());
                preparedStatement.setString(++i, dateDimension.getDay());
            } else if (dimension instanceof ContactDimension) {
                ContactDimension contactDimension = (ContactDimension) dimension;
                preparedStatement.setString(++i, contactDimension.getUserId());
                preparedStatement.setString(++i, contactDimension.getUserName());
            } else if (dimension instanceof ProductDimension) {
                ProductDimension productDimension = (ProductDimension) dimension;
                preparedStatement.setString(++i, productDimension.getProductName());
                preparedStatement.setString(++i, productDimension.getPrice());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回用户表的查询和插入语句
     */
    private String[] getContactDimensionSQL() {
        String query = "SELECT `id` FROM `tb_contacts` WHERE `user_id` = ? AND `user_name` = CONVERT( ? USING utf8) COLLATE utf8_unicode_ci ORDER BY `id`;";
        String insert = "INSERT INTO `tb_contacts` (`user_id`, `user_name`) VALUES (?, CONVERT( ? USING utf8) COLLATE utf8_unicode_ci);";
        return new String[]{query, insert};
    }

    /**
     * 返回时间表的查询和插入语句
     */
    private String[] getDateDimensionSQL() {
        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND `month` = ? AND `day` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`, `month`, `day`) VALUES (?, ?, ?);";
        return new String[]{query, insert};
    }

    /**
     * 返回商品表的查询和插入语句
     */
    private String[] getProductDimensionSQL() {
        String query = "SELECT `id` FROM `tb_products` WHERE `product_name` = CONVERT( ? USING utf8) COLLATE utf8_unicode_ci AND `price` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_products` (`product_name`, `price`) VALUES (CONVERT( ? USING utf8) COLLATE utf8_unicode_ci, ?);";
        return new String[]{query, insert};
    }

    /**
     * 根据维度信息得到维度对应的缓存键
     */
    private String genCacheKey(BaseDimension dimension) {
        StringBuilder sb = new StringBuilder();
        if (dimension instanceof DateDimension) {
            DateDimension dateDimension = (DateDimension) dimension;
            sb.append("date_dimension")
                    .append(dateDimension.getYear())
                    .append(dateDimension.getMonth())
                    .append(dateDimension.getDay());
        } else if (dimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) dimension;
            sb.append("contact_dimension")
                    .append(contactDimension.getUserId())
                    .append("_")
                    .append(contactDimension.getUserName());
        } else if (dimension instanceof ProductDimension) {
            ProductDimension productDimension = (ProductDimension) dimension;
            sb.append("product_dimension")
                    .append(productDimension.getProductName())
                    .append("_")
                    .append(productDimension.getPrice());
        }
        return sb.toString();
    }
}
