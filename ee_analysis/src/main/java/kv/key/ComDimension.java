package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComDimension extends BaseDimension {
    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();
    private ProductDimension productDimension = new ProductDimension(); // 新增商品维度

    public ComDimension(){
        super();
    }

    // 添加构造方法，支持联合维度的便捷创建
    public ComDimension(ContactDimension contactDimension, DateDimension dateDimension) {
        super();
        this.contactDimension = contactDimension;
        this.dateDimension = dateDimension;
    }

    // 添加构造方法，支持包含商品维度的联合维度创建
    public ComDimension(ContactDimension contactDimension, DateDimension dateDimension,
                        ProductDimension productDimension) {
        super();
        this.contactDimension = contactDimension;
        this.dateDimension = dateDimension;
        this.productDimension = productDimension;
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public ProductDimension getProductDimension() {
        return productDimension;
    }

    public void setProductDimension(ProductDimension productDimension) {
        this.productDimension = productDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        ComDimension anotherComDimension = (ComDimension) o;

        int result = this.dateDimension.compareTo(anotherComDimension.dateDimension);
        if(result != 0) return result;

        result = this.contactDimension.compareTo(anotherComDimension.contactDimension);
        if(result != 0) return result;

        result = this.productDimension.compareTo(anotherComDimension.productDimension);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        contactDimension.write(out);
        dateDimension.write(out);
        productDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        contactDimension.readFields(in);
        dateDimension.readFields(in);
        productDimension.readFields(in);
    }
}
