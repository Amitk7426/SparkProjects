package com.myCompany.javaExample.Utils;

import java.io.Serializable;

public class CustomerPayment implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 310674578645707372L;
	private String customerName;
	private String accountNumber;
	private String total;
	
	public String getCustomerName() {
		return customerName;
	}
	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}
	public String getAccountNumber() {
		return accountNumber;
	}
	public void setAccountNumber(String accountNumber) {
		this.accountNumber = accountNumber;
	}
	public String getTotal() {
		return total;
	}
	public void setTotal(String total) {
		this.total = total;
	}
}
