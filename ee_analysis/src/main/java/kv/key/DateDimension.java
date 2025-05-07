package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateDimension extends BaseDimension {
    private String year;
    private String month;
    private String day;

    // 用于解析标准格式的日期时间
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // 用于提取年月日
    private static final SimpleDateFormat YEAR_MONTH_DAY = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat YEAR_FORMAT = new SimpleDateFormat("yyyy");// 用于提取年份
    private static final SimpleDateFormat MONTH_FORMAT = new SimpleDateFormat("MM");
    private static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("dd");

    public DateDimension(){
        super();// 调用父类的无参构造函数
    }

    // 用于创建日期维度对象
    public DateDimension(String year, String month, String day){
        super();
        this.year = year;
        this.month = month;
        this.day = day;
    }

    /**
     * 从订单时间创建日期维度对象
     * @param orderTime 格式为 "yyyy-MM-dd HH:mm:ss" 的订单时间
     * @return 日期维度对象
     * @throws ParseException 如果订单时间格式不正确
     */
    public static DateDimension fromOrderTime(String orderTime) {
        try {
            Date date = SDF.parse(orderTime);//将订单时间解析为Date对象
            String year = YEAR_FORMAT.format(date);// 提取年份
            String month = MONTH_FORMAT.format(date);
            String day = DAY_FORMAT.format(date);
            return new DateDimension(year, month, day);// 返回日期维度对象
        } catch (ParseException e) {
            // 解析失败时返回空对象
            return new DateDimension("0000", "00", "00");// 返回空对象
        }
    }

    /**
     * 创建月度维度对象（天设为"00"表示整月）
     * @param year 年份
     * @param month 月份
     * @return 月度维度对象
     */
    public static DateDimension createMonthDimension(String year, String month) {
        return new DateDimension(year, month, "00");// 返回月度维度对象
    }

    /**
     * 从当前日期维度创建月度维度
     * @return 月度维度对象
     */
    public DateDimension toMonthDimension() {
        return createMonthDimension(this.year, this.month);
    }

    /**
     * 判断是否为月度维度
     * @return 是否为月度维度
     */
    public boolean isMonthDimension() {
        return "00".equals(this.day);//判断天是否为"00"
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    // 重写equals和hashCode方法
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateDimension that = (DateDimension) o;

        if (year != null ? !year.equals(that.year) : that.year != null) return false;
        if (month != null ? !month.equals(that.month) : that.month != null) return false;
        return day != null ? day.equals(that.day) : that.day == null;
    }

    @Override
    public int hashCode() {//保证hash值唯一
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (month != null ? month.hashCode() : 0);// 31是质数，用于散列计算
        result = 31 * result + (day != null ? day.hashCode() : 0);//
        return result;
    }

    // 重写compareTo方法
    @Override
    public int compareTo(BaseDimension o) {
        DateDimension anotherDateDimension = (DateDimension)o;

        // 添加null检查，避免NPE
        String thisYear = this.year != null ? this.year : "0000";
        String thatYear = anotherDateDimension.year != null ? anotherDateDimension.year : "0000";

        int result = thisYear.compareTo(thatYear);// 比较年
        if(result != 0) return result;

        // 添加null检查，避免NPE
        String thisMonth = this.month != null ? this.month : "00";
        String thatMonth = anotherDateDimension.month != null ? anotherDateDimension.month : "00";

        result = thisMonth.compareTo(thatMonth);
        if(result != 0) return result;

        // 添加null检查，避免NPE
        String thisDay = this.day != null ? this.day : "00";
        String thatDay = anotherDateDimension.day != null ? anotherDateDimension.day : "00";

        result = thisDay.compareTo(thatDay);

        return result;
    }

    // 重写write方法，实现序列化
    @Override
    public void write(DataOutput out) throws IOException {
        // 添加null检查，写入默认值而不是null
        out.writeUTF(this.year != null ? this.year : "0000");// 写入年
        out.writeUTF(this.month != null ? this.month : "00");
        out.writeUTF(this.day != null ? this.day : "00");
    }

    // 重写readFields方法，实现反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readUTF();// 读取年
        this.month = in.readUTF();
        this.day = in.readUTF();
    }
}