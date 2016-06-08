package com.zjlp.face.spark.base;

import org.apache.spark.sql.SQLContext;

/**
 * Title: ISparkBaseFactory.class<br>
 * Description:基类接口 <br>
 * Copyright (c) 版权所有 2016    <br>
 * Create DateTime: 2016年05月12日 11:42 <br>
 *
 * @author wanghuanhuan
 */
public interface ISparkBaseFactory {

    /**
     * 获取sqlContext
     * @return 返回值
     */
    public SQLContext getSQLContext();

    /**
     * 更新数据源
     */
    public  void updateSQLContext();


}
