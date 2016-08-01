package com.zjlp.face.spark.bean;

/**
 * Created by root on 7/28/16.
 */
import java.io.Serializable;

/**
 * 行业人脉 Dto
 * Created by linel on 2016-7-13.
 */
public class IndustryConnectionsDto implements Serializable {

    private static final long serialVersionUID = -1946547555834287197L;

    /**
     * 用户ID
     */
    private Long              userId;
    /**
     * 用户登录帐号
     */
    private String            loginAccount;
    /**
     *头像
     */
    private String            headImgUrl;
    /**
     * 实名认证否 1 是，0 否，默认否
     */
    private Integer           certify          = 0;
    /**
     * 名字
     */
    private String            contacts;
    /**
     * 威望指
     */
    private Integer           prestigeAmount;
    /**
     * 职务
     */
    private String            position;
    /**
     * 公司名
     */
    private String            companyName;
    /**
     * 地区：省+市
     */
    private String            vAreaName;
    /**
     * 供应信息
     */
    private String            industryProvide;
    /**
     * 需求信息
     */
    private String            industryRequirement;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getLoginAccount() {
        return loginAccount;
    }

    public void setLoginAccount(String loginAccount) {
        this.loginAccount = loginAccount;
    }

    public String getHeadImgUrl() {
        return headImgUrl;
    }

    public void setHeadImgUrl(String headImgUrl) {
        this.headImgUrl = headImgUrl;
    }

    public Integer getCertify() {
        return certify;
    }

    public void setCertify(Integer certify) {
        this.certify = certify;
    }

    public String getContacts() {
        return contacts;
    }

    public void setContacts(String contacts) {
        this.contacts = contacts;
    }

    public Integer getPrestigeAmount() {
        return prestigeAmount;
    }

    public void setPrestigeAmount(Integer prestigeAmount) {
        this.prestigeAmount = prestigeAmount;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getvAreaName() {
        return vAreaName;
    }

    public void setvAreaName(String vAreaName) {
        this.vAreaName = vAreaName;
    }

    public String getIndustryProvide() {
        return industryProvide;
    }

    public void setIndustryProvide(String industryProvide) {
        this.industryProvide = industryProvide;
    }

    public String getIndustryRequirement() {
        return industryRequirement;
    }

    public void setIndustryRequirement(String industryRequirement) {
        this.industryRequirement = industryRequirement;
    }

    @Override
    public String toString() {
        return userId.toString();
    }
}
