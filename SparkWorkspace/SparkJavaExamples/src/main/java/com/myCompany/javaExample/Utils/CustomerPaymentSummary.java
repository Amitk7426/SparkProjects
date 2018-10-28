package com.myCompany.javaExample.Utils;

import java.io.Serializable;
import java.util.List;

public class CustomerPaymentSummary implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 310674578645707372L;
	private List<CustomerPayment> customers;
	public List<CustomerPayment> getCustomers() {
		return customers;
	}
	public void setCustomers(List<CustomerPayment> customers) {
		this.customers = customers;
	}
	
}
