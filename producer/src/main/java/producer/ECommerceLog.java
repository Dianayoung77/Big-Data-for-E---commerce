package producer;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class ECommerceLog {

    private String startTime = "2024-01-01";
    private String endTime = "2024-12-31";

    // 创建一个ArrayList用于存放待随机的用户ID
    private List<String> userList = new ArrayList<>();
    // 创建一个Map结构用于存放用户ID和用户名
    private Map<String, String> userNameMap = new HashMap<>();
    // 商品列表
    private List<String> productList = new ArrayList<>();
    // 商品价格
    private Map<String, Double> productPriceMap = new HashMap<>();

    public void initData() {
        // 用户列表初始化
        userList.add("2022064074");
        userList.add("2022064075");
        userList.add("2022064073");
        userList.add("user1004");
        userList.add("user1005");

        userNameMap.put("2022064074", "杨敏");
        userNameMap.put("user1002", "孙洁");
        userNameMap.put("2022064073", "王天慧");
        userNameMap.put("user1004", "陈心圆");
        userNameMap.put("user1005", "孙七");

        // 商品列表初始化
        productList.add("phone");
        productList.add("computer");
        productList.add("watercup");
        productList.add("shoes");
        productList.add("hairpin");

        productPriceMap.put("phone", 999.99);
        productPriceMap.put("computer", 11149.49);
        productPriceMap.put("watercup", 79.99);
        productPriceMap.put("shoes", 199.99);
        productPriceMap.put("hairpin", 49.99);
    }

    public String randomBuildDate(String startTime, String endTime) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");//转换为DATE对象，以便后续计算时间线戳
            Date startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);

            if (endDate.getTime() <= startDate.getTime()) return null;
            // 利用random，结束时间-开始时间*随机random，加上开始时间，得到2024年随机的一个时间
            long randomTS = startDate.getTime() +
                    (long) ((endDate.getTime() - startDate.getTime()) * Math.random());//将总毫秒数乘以随机小数，得到时间范围内的随机偏移量，获取随机时间戳
            Date result = new Date(randomTS);//将随机时间戳转换为DATE对象
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//转换为DATE对象，以便后续计算时间线戳
            String resulTimeString = sdf2.format(result);//将DATE对象转换为字符串
            return resulTimeString;//
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String generateOrder() {
        String userId = null;//用户ID
        String userName = null;
        String productName = null;
        double price = 0.0;
        int quantity = 0;

        // 随机选择用户
        int userIndex = (int) (userList.size() * Math.random());//获取用户列表的长度，乘以随机数，得到随机索引
        userId = userList.get(userIndex);//根据随机索引获取用户列表中的用户ID
        userName = userNameMap.get(userId);//根据用户ID获取用户名

        // 随机选择商品
        int productIndex = (int) (productList.size() * Math.random());
        productName = productList.get(productIndex);
        price = productPriceMap.get(productName);

        // 随机选择购买数量
        quantity = (int) (1 + Math.random() * 5);  // 随机数量在1到5之间

        // 随机生成订单时间
        String orderTime = randomBuildDate(startTime, endTime);//调用randomBuildDate方法，生成订单时间

        // 拼接订单数据
        StringBuilder sb = new StringBuilder();
        sb.append(userId).append(",")
                .append(userName).append(",")
                .append(productName).append(",")
                .append(quantity).append(",")
                .append(price).append(",")
                .append(price * quantity).append(",")
                .append(orderTime);

        return sb.toString();//返回订单数据
    }

    public void writeLog(String filePath) {
        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath));//创建文件输出流
            while (true) {
                Thread.sleep(1000); // 防止数据生产过快
                String log = generateOrder(); // 调用generateOrder生成一条订单数据
                System.out.println(log); // 显示
                osw.write(log + "\n"); // 将生产数据放到目标文件
                osw.flush(); // 手动落盘
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//  是数据生产低开于1000条
//    public void writeLog(String filePath) {
//        try {
//            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath));
//            int count = 0;
//            while (count < 1000) { // 最多生成1000条
//                Thread.sleep(1000); // 防止数据生产过快
//                String log = generateOrder(); // 调用generateOrder生成一条订单数据
//                System.out.println(log); // 显示
//                osw.write(log + "\n"); // 将生产数据放到目标文件
//                osw.flush(); // 手动落盘
//                count++;
//            }
//            osw.close(); // 关闭文件流
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }


    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            System.out.println("你没有输入数据生产路径参数：");
            return;
        }

        ECommerceLog eCommerceLog = new ECommerceLog();//初始化
        eCommerceLog.initData();//初始化数据
        eCommerceLog.writeLog(args[0]); // 接受第一个参数
    }
}
