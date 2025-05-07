package kv.value;

import kv.base.BaseValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 商品购买统计值类
 * 用于存储购买数量和金额统计信息
 */
public class CountDurationValue extends BaseValue {
    private String orderCount;      // 订单数量/购买频率
    private String totalQuantity;   // 商品总数量
    private String totalAmount;     // 总金额

    public CountDurationValue() {
        super();
        this.orderCount = "0";// 订单数量/购买频率
        this.totalQuantity = "0";
        this.totalAmount = "0.0";
    }

    public CountDurationValue(String orderCount, String totalQuantity, String totalAmount) {
        super();
        this.orderCount = orderCount;
        this.totalQuantity = totalQuantity;
        this.totalAmount = totalAmount;
    }

    public String getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(String orderCount) {
        this.orderCount = orderCount;
    }

    public String getTotalQuantity() {
        return totalQuantity;
    }

    public void setTotalQuantity(String totalQuantity) {
        this.totalQuantity = totalQuantity;
    }

    public String getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(String totalAmount) {
        this.totalAmount = totalAmount;
    }

    /**
     * 将值累加到当前对象
     * @param other 另一个统计值对象
     */
    // CountDurationValue 内置 add() 方法，便于 Reducer 对相同键的值进行累加统计。
    public void add(CountDurationValue other) {
        try {
            this.orderCount = String.valueOf(Integer.parseInt(this.orderCount) +
                    Integer.parseInt(other.orderCount));// 订单数累加

            this.totalQuantity = String.valueOf(Integer.parseInt(this.totalQuantity) +
                    Integer.parseInt(other.totalQuantity));// 数量累加

            this.totalAmount = String.valueOf(Double.parseDouble(this.totalAmount) +
                    Double.parseDouble(other.totalAmount));// 金额累加
        } catch (NumberFormatException e) {
            // 处理数字格式异常
        }
    }

    @Override
    // 序列化实现
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderCount);
        out.writeUTF(totalQuantity);
        out.writeUTF(totalAmount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderCount = in.readUTF();
        this.totalQuantity = in.readUTF();
        this.totalAmount = in.readUTF();
    }
}
