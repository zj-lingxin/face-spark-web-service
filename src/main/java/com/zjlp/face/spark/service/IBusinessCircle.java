package com.zjlp.face.spark.service;

import com.zjlp.face.spark.bean.CommonFriendNum;
import com.zjlp.face.spark.bean.PersonRelation;
import org.apache.spark.sql.DataFrame;

import java.util.List;

public interface IBusinessCircle {


    /**
     * 一度好友查询
     * @param loginAccount 用户ID
     * @return 返回结果集
     */
    public DataFrame searchFriendFrame(String loginAccount);

    /**
     * 二度好友查询
     * @param loginAccount 用户ID
     * @return 返回结果集
     */
    public DataFrame searchTwoFriendFrame(String loginAccount);

    /**
     * 根据当前登录用户id和用户id列表查询共同好友数
     * 
     * @param userNames 附近店铺用户集
     * @param loginAccount 登入账号
     * @return 返回结果集
     */
    public List<CommonFriendNum> searchCommonFriendNum(List<String> userNames, String loginAccount);
    
    /**
     * 根据当前登录用户id和用户id列表返回人脉关系类型列表
     * 
     * @param userIds 用户集
     * @param loginAccount 登入账号
     * @return
     */
    public List<PersonRelation> searchPersonRelation(List<String> userIds, String loginAccount);

    /**
     * 更新数据源 定时任务调取
     * @return 返回执行状态
     */
    public Boolean updateDBSources();
}
