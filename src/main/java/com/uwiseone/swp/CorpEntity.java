package com.uwiseone.swp;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class CorpEntity {
	private String corpPk;
	private String corpName;
	private String region;
	private String empCntLimit;
	private String saleAmtLimit;
	
	public String getCorpPk() {
		return corpPk;
	}
	public void setCorpPk(String corpPk) {
		this.corpPk = corpPk;
	}
	public String getCorpName() {
		return corpName;
	}
	public void setCorpName(String corpName) {
		this.corpName = corpName;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getEmpCntLimit() {
		return empCntLimit;
	}
	public void setEmpCntLimit(String empCntLimit) {
		this.empCntLimit = empCntLimit;
	}
	public String getSaleAmtLimit() {
		return saleAmtLimit;
	}
	public void setSaleAmtLimit(String saleAmtLimit) {
		this.saleAmtLimit = saleAmtLimit;
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
	}
}
