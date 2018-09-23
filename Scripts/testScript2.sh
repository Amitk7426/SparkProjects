#!/bin/bash
spark-submit --class com.myCompany.scalaExample.MaxPrice --master local --executor-memory 1g C:\\Users\\Amit\\Desktop\\TestSpark\\sparkExample\\target/sparkExample-1.0-SNAPSHOT-jar-with-dependencies.jar C:\\Users\\Amit\\Desktop\\TestSpark\\sparkExample\\input\\table.csv
C:\\Users\\Amit\\Desktop\\TestSpark\\sparkExample\\output