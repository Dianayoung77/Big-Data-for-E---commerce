package kv.key;

import kv.base.BaseDimension;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 商品维度类
 * 用于分析商品销售情况
 */
public class ProductDimension extends BaseDimension {
    private String productName;
    private String price;

    public ProductDimension() {
        super();
    }

    public ProductDimension(String productName, String price) {
        super();
        this.productName = productName;
        this.price = price;
    }

    /**
     * 仅使用商品名创建商品维度对象
     * @param productName 商品名称
     * @return 商品维度对象
     */
    public static ProductDimension create(String productName) {
        return new ProductDimension(productName, "0.0");
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProductDimension that = (ProductDimension) o;

        if (productName != null ? !productName.equals(that.productName) : that.productName != null) return false;
        return price != null ? price.equals(that.price) : that.price == null;
    }

    @Override
    public int hashCode() {
        int result = productName != null ? productName.hashCode() : 0;
        result = 31 * result + (price != null ? price.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        ProductDimension anotherProductDimension = (ProductDimension) o;

        // 修改为更安全的null处理方式
        String thisProductName = this.productName != null ? this.productName : "";
        String thatProductName = anotherProductDimension.productName != null ?
                anotherProductDimension.productName : "";

        int result = thisProductName.compareTo(thatProductName);
        if (result != 0) return result;

        // 修改为更安全的null处理方式
        String thisPrice = this.price != null ? this.price : "0.0";
        String thatPrice = anotherProductDimension.price != null ?
                anotherProductDimension.price : "0.0";

        result = thisPrice.compareTo(thatPrice);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write方法已有null检查，但为了一致性和清晰度，稍作修改
        out.writeUTF(this.productName != null ? this.productName : "");
        out.writeUTF(this.price != null ? this.price : "0.0");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.productName = in.readUTF();
        this.price = in.readUTF();
    }
}
