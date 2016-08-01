package com.zjlp.face.spark.bean;

import java.io.Serializable;
import java.util.List;

/**
 * Created by root on 7/28/16.
 */
public class IndustryConnectionsResult implements Serializable {
    private static final long serialVersionUID = -1946547555834288456L;

    private long count;
    private List<IndustryConnectionsDto> industryConnectionsList;

    public List<IndustryConnectionsDto> getIndustryConnectionsList() {
        return industryConnectionsList;
    }

    public void setIndustryConnectionsList(List<IndustryConnectionsDto> industryConnectionsList) {
        this.industryConnectionsList = industryConnectionsList;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
