package com.myCompany.javaExample.Utils;

import java.io.Serializable;

public class PaymentType implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4124276897198543886L;
	private String payment;
	private String total_price;
	public String getPayment() {
		return payment;
	}
	public void setPayment(String payment) {
		this.payment = payment;
	}
	public String getTotal_price() {
		return total_price;
	}
	public void setTotal_price(String total_price) {
		this.total_price = total_price;
	}
}