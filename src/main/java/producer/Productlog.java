package producer;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Productlog {

    private String startTime = "2024-01-01";
    private String endTime = "2024-12-31";

    //创建一个ArrayList用于存放待随机的电话号码
    private List<String> phoneList = new ArrayList<>();
    //创建一个Map结构用于存放待随机的电话号码和主机名称
    private Map<String, String> phoneNameMap = new HashMap<>();


    public void initPhone() {

        phoneList.add("18078386992");
        phoneList.add("13880337439");
        phoneList.add("14575535933");
        phoneList.add("19902496992");
        phoneList.add("18549641558");
        phoneList.add("17005930322");
        phoneList.add("18468618874");
        phoneList.add("18576581848");
        phoneList.add("15978226424");
        phoneList.add("15542823911");
        phoneList.add("17526304161");
        phoneList.add("15422018558");
        phoneList.add("17269452013");
        phoneList.add("17764278604");
        phoneList.add("15711910344");
        phoneList.add("15714728273");
        phoneList.add("16061028454");
        phoneList.add("16264433631");
        phoneList.add("17601615878");
        phoneList.add("15897468949");


        phoneNameMap.put("18078386992", "甄守军");
        phoneNameMap.put("13880337439", "于桐");
        phoneNameMap.put("14575535933", "韩新闻");
        phoneNameMap.put("19902496992", "吴锦涛");
        phoneNameMap.put("18549641558", "牛方硕");
        phoneNameMap.put("17005930322", "张硕");
        phoneNameMap.put("18468618874", "姜文可");
        phoneNameMap.put("18576581848", "王宾利");
        phoneNameMap.put("15978226424", "王安康");
        phoneNameMap.put("15542823911", "吕云龙");
        phoneNameMap.put("17526304161", "唐文轩");
        phoneNameMap.put("15422018558", "孙怡蓉");
        phoneNameMap.put("17269452013", "孙婷");
        phoneNameMap.put("17764278604", "李志浩");
        phoneNameMap.put("15711910344", "秦梦秋");
        phoneNameMap.put("15714728273", "刘豪林");
        phoneNameMap.put("16061028454", "孔伟豪");
        phoneNameMap.put("16264433631", "张洪泰");
        phoneNameMap.put("17601615878", "朱珠");
        phoneNameMap.put("15897468949", "刘晨旭");

    }

    public String randomBuildDate(String startTime, String endTime) {

        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);

            if (endDate.getTime() <= startDate.getTime()) return null;
            //利用random，结束时间-开始时间*随机random，加上开始时间，得到2024年随机的一个时间
            long randomTS = startDate.getTime() +
                    (long) ((endDate.getTime() - startDate.getTime()) * Math.random());
            Date result = new Date(randomTS);
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String resulTimeString = sdf2.format(result);
            return resulTimeString;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public String product() {
        String caller = null;
        String callee = null;
        String callerName = null;
        String calleeName = null;
        //取得主叫电话号码  随机
        int callerIndex = (int) (phoneList.size() * Math.random());
        caller = phoneList.get(callerIndex);
        callerName = phoneNameMap.get(caller);
        while (true) {
            int calleeIndex = (int) (phoneList.size() * Math.random());
            callee = phoneList.get(calleeIndex);
            calleeName = phoneNameMap.get(callee);
            if (!caller.equals(callee)) break;
        }

        //随机通话时长
        String buildTime = randomBuildDate(startTime, endTime);
        //随机生成通话时长
        DecimalFormat df = new DecimalFormat("0000");
        String duration = df.format((int) (30 * 60 * Math.random()));
        //拼接
        StringBuilder sb = new StringBuilder();
        sb.append(caller + ",").append(callee + ",").append(buildTime+",").append(duration+",");
        //.append(callerIndex + ",");
        return sb.toString();
    }


    public void wrtielog(String filePath) {
        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath));
            while (true) {
                Thread.sleep(1000);//防止数据生产过快
                String log = product();//条用product生产一条数据
                System.out.println(log);//显示
                osw.write(log);//将生产数据放到目标文件
                osw.flush();//手动落盘
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static  void main(String[] args){
        if (args== null || args.length < 1) {
            System.out.println("你没有输入数据生产路径参数：");

        }
        Productlog productlog =new Productlog();
        productlog.initPhone();
        productlog.wrtielog(args[0]);//接受第一个参数

//        String logPath ="C:\\Users\\Diana\\IdeaProjects\\sociaty_project\\calllog.csv";
//        productlog.wrtielog(logPath);
    }


}


