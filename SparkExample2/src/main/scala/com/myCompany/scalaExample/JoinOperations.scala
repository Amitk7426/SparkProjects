package com.myCompany.scalaExample

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object JoinOperations {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MaxPrice").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val emp = sc.parallelize(Seq(("jordan",10), ("ricky",20), ("matt",30), ("mince",35), ("rhonda",30)))


    val dept = sc.parallelize(Seq(("hadoop",10), ("spark",20), ("hive",30), ("sqoop",40)))

    val shifted_fields_emp = emp.map(t => (t._2, t._1))

    val shifted_fields_dept = dept.map(t => (t._2,t._1))

    shifted_fields_emp.join(shifted_fields_dept).collect().foreach(x=>println(x))

    shifted_fields_emp.cogroup(shifted_fields_dept).collect().foreach(x=>println(x))

   /* // Create emp RDD
    val emp1 = sc.parallelize(Seq((1,"jordan",10), (2,"ricky",20), (3,"matt",30), (4,"mince",35), (5,"rhonda",30)))

    // Create dept RDD
    val dept1 = sc.parallelize(Seq(("hadoop",10), ("spark",20), ("hive",30), ("sqoop",40)))

    // Establishing that the third field is to be considered as the Key for the emp RDD
    val manipulated_emp = emp1.keyBy(t => t._3)

    // Establishing that the second field need to be considered as the Key for dept RDD
    val manipulated_dept = dept1.keyBy(t => t._2)

    // Inner Join
    val join_data = manipulated_emp.join(manipulated_dept)
    join_data.collect().foreach(x=>println(x))
    // Left Outer Join
    val left_outer_join_data = manipulated_emp.leftOuterJoin(manipulated_dept)
    left_outer_join_data.collect().foreach(x=>println(x))
    // Right Outer Join
    val right_outer_join_data = manipulated_emp.rightOuterJoin(manipulated_dept)
    right_outer_join_data.collect().foreach(x=>println(x))
    // Full Outer Join
    val full_outer_join_data = manipulated_emp.fullOuterJoin(manipulated_dept)
    full_outer_join_data.collect().foreach(x=>println(x))

    // Formatting the Joined Data for better understandable (using map)
    val cleaned_joined_data = join_data.map(t => (t._2._1._1, t._2._1._2, t._1, t._2._2._1))
    cleaned_joined_data.collect().foreach(x=>println(x))*/

    sc.stop
  }
}
