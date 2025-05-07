package utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 基于LRU（最近最少使用）算法的缓存实现
 * 适用于缓存热门商品、活跃用户等电商分析中的频繁访问数据
 * @param <K> 缓存键的类型
 * @param <V> 缓存值的类型
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = 1L;
    protected int maxElements;

    /**
     * 构造具有指定大小的LRU缓存
     * @param maxSize 缓存的最大容量
     */
    public LRUCache(int maxSize) {
        // 初始容量、负载因子、访问顺序（true表示按访问顺序，false表示按插入顺序）
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    /**
     * 决定是否移除最老的元素
     * 当缓存大小超过最大容量时移除最老（最少使用）的元素
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return (size() > this.maxElements);
    }

    /**
     * 添加或更新缓存中的值，并返回该值
     * @param key 键
     * @param value 值
     * @return 添加或更新后的值
     */
    public V putAndGet(K key, V value) {
        put(key, value);
        return value;
    }

    /**
     * 清除所有缓存内容
     */
    public void clearCache() {
        clear();
    }
}
