package com.zjlp.face.spark.service;

import com.zjlp.face.spark.bean.IndustryConnectionsResult;

public interface IIndustryContacts {

    public IndustryConnectionsResult getContacts(Long userId, Integer myIndustryCode, Integer areaCode, int[] industryCodes, int pageNo, int pageSize);

    public IndustryConnectionsResult getContacts(Long userId, int prestigeAmount, int[] areaCodes, int[] industryCodes, int pageNo, int pageSize);
}
