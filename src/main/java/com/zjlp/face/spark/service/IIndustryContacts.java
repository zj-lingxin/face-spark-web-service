package com.zjlp.face.spark.service;

import com.zjlp.face.spark.bean.IndustryConnectionsResult;

public interface IIndustryContacts {

    public IndustryConnectionsResult getContacts(Long userId, int myIndustryCode, int areaCode, int[] industryCodes, int pageNo, int pageSize);

}
