package com.myCompany.javaExample.Utils;

import java.io.Serializable;

public class ProductType implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5770220071786058551L;
	private String product;
	private String total_price;
	public String getProduct() {
		return product;
	}
	public void setProduct(String product) {
		this.product = product;
	}
	public String getTotal_price() {
		return total_price;
	}
	public void setTotal_price(String total_price) {
		this.total_price = total_price;
	}
	
}