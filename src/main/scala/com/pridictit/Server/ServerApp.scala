package com.pridictit.Server

import com.typesafe.config.ConfigFactory
import scala.collection.immutable.HashMap
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._
import scala.collection.Map
import org.apache.spark.sql.internal.SharedState
import scala.util.Success
import scala.util.Failure
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

import com.pridictit.main.UserInfor
import com.pridictit.hive.Parquet
import com.pridictit.hive.GetData
import com.pridictit.hive.StaticDataFrames

import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.json4s.native.Serialization._
import org.json4s.native.Serialization

class MessageExecuter {       
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val hiveMetastore = "file:${system:user.dir}/Hive-Warehouse"
    //Class.forName("com.mysql.jdbc.Driver")
      val spark = SparkSession
        .builder()
        .master("local[4]")
        .appName("Pridict_It")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        //.config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$warehouseLocation;create=true")
        //.config("hive.metastore.warehouse.dir", warehouseLocation)
        //.config("datanucleus.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
        //.config(ConfVars.METASTOREURIS.varname, "")
        //.config("javax.jdo.option.ConnectionDriverName","com.mysql.jdbc.Driver")
        //.config(confvar.varname, confvar.getDefaultExpr())
        //.config("spark.sql.hive.thriftServer.singleSession",true)
        .enableHiveSupport()
        .getOrCreate()
    
    val sc = spark.sparkContext 
   
}

object ServerApp extends App {
  implicit val formats = Serialization.formats(NoTypeHints)
  var msg  = new java.util.HashMap[String,Object]()
  var msg1  = new java.util.HashMap[String,Object]()
  var msg3  = new java.util.HashMap[String,Object]()
  var msg2 = new java.util.HashMap[String,java.util.ArrayList[java.util.HashMap[String,Object]]]();
  val array = new java.util.ArrayList[Object]()
  array.add("Md")
  array.add("mb_lg")
  msg1.put("magType",array)
  val array1 = new java.util.ArrayList[Object]()
  array1.add("2.0")
  array1.add("2.33")
  var array2 = new java.util.ArrayList[java.util.HashMap[String,Object]]();
  array2.add(msg1)
  //msg3.put("rms",array1)
 // msg1.put("mag",array)
  msg2.put("String", array2)
 // msg2.put("numeric", msg3)
  msg.put("tablename", "allmonths")
  msg.put("delimiter", ",")
  msg.put("csvFile" , "/home/srikanth/drives/B/poc/earthquake/all_month.csv")  
  msg.put("dsName", "allmonths")
  msg.put("predicate", "time")
  
  val parquet = new Parquet()
  val getData = new GetData()
  val result = getData.getTableData(msg2, "allmonths")
  
  
  
  
}
