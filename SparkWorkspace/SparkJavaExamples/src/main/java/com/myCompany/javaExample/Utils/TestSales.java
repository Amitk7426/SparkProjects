package com.myCompany.javaExample.Utils;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.Row;

import scala.util.parsing.combinator.testing.Str;

public class TestSales implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6108351483558072200L;
	private List<PaymentType> payment_Type;
	private List<ProductType> product_Type;
	public List<ProductType> getProduct_Type() {
		return product_Type;
	}
	public void setProduct_Type(List<ProductType> product_Type) {
		this.product_Type = product_Type;
	}
	public List<PaymentType> getPayment_Type() {
		return payment_Type;
	}
	public void setPayment_Type(List<PaymentType> payment_Type) {
		this.payment_Type = payment_Type;
	}
	
}

