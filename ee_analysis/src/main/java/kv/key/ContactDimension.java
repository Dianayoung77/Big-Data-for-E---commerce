package kv.key;
import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//继承 BaseDimension
public class ContactDimension extends BaseDimension {
    private String userId;
    private String userName;

    public ContactDimension() {
        super();//调用父类的构造方法
    }

    public ContactDimension(String userId, String userName) {
        super();
        this.userId = userId;
        this.userName = userName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public boolean equals(Object o) {//重写equals方法
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;//判断是否为同一个类

        ContactDimension that = (ContactDimension) o;//强制类型转换

        if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
        return userName != null ? userName.equals(that.userName) : that.userName == null;
    }

    @Override
    public int hashCode() {//重写hashCode方法
        int result = userId != null ? userId.hashCode() : 0;
        result = 31 * result + (userName != null ? userName.hashCode() : 0);//31是质数，用于散列计算
        return result;
    }
    // 排序规则：先比较用户名，再比较用户ID
    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension anotherContactDimension = (ContactDimension) o;

        // 添加null检查处理，避免NPE
        String thisUserName = this.userName != null ? this.userName : "";// 添加null检查处理，避免NPE
        String thatUserName = anotherContactDimension.userName != null ? anotherContactDimension.userName : "";// 添加null检查处理，避免NPE

        int result = thisUserName.compareTo(thatUserName);// 比较用户名
        if (result != 0) return result;

        // 添加null检查处理，避免NPE
        String thisUserId = this.userId != null ? this.userId : "";
        String thatUserId = anotherContactDimension.userId != null ? anotherContactDimension.userId : "";

        result = thisUserId.compareTo(thatUserId);// 比较用户ID
        return result;
    }
//序列化实现
    @Override
    public void write(DataOutput out) throws IOException {//
        // 添加null检查，写入空字符串而不是null值
        out.writeUTF(this.userId != null ? this.userId : "");
        out.writeUTF(this.userName != null ? this.userName : "");
    }
//反序列化实现
    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readUTF();
        this.userName = in.readUTF();
    }
}
