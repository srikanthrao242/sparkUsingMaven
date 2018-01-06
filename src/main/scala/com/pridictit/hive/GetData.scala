package com.pridictit.hive

import com.pridictit.Server.MessageExecuter


import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Encoder
import scala.collection.Map
import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import scala.collection.immutable.HashMap
import org.apache.spark.sql.functions._

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.json4s.native.Serialization._
import org.json4s.native.Serialization

class GetData {
  
  val config = ConfigFactory.load()
  implicit val formats = Serialization.formats(NoTypeHints)
  def getDataSources() : java.util.HashMap[String,Object] ={
    
    val javaHashMap = new java.util.HashMap[String,Object]()    
    try{
      val metaDB = config.getString("databases.metadb")
      var messageExecuter = new MessageExecuter()
      val spark : SparkSession = messageExecuter.spark
      val sqlDF = spark.sql("SELECT db_name FROM  "
                                     +metaDB+".database_info ");
      val hashMap = new java.util.HashMap[String,Object]()
      import spark.sqlContext.implicits._
      var result = sqlDF.columns.map(col => {
       hashMap.put(col ,
       sqlDF.groupBy(col)
         .count()
         .map(_.getString(0))
         .collect())
       })
       var da = hashMap.get("db_name").asInstanceOf[Array[String]]
      //var result1 = result.asInstanceOf[Array[java.util.Map[String,Object]]]
      javaHashMap.put("data", hashMap)
      javaHashMap.put("msg", "Success")
      println("Successfull  createDataSourceInfo ")    
      javaHashMap 
    }catch{
      case e:Exception => {javaHashMap.put("msg", "Error")       
        javaHashMap.put("error", e.toString()) 
        println("UnSuccessfull  createDataSourceInfo ")    
      javaHashMap}
    } 
  }
  def getCategories(datasource:String): java.util.HashMap[String,Object] = {
    val javaHashMap = new java.util.HashMap[String,Object]()    
    try{
      val metaDB = config.getString("databases.metadb")
      var messageExecuter = new MessageExecuter()
      val spark : SparkSession = messageExecuter.spark
      val sqlDFString = spark.sql("SELECT category FROM  "
                                     +metaDB+".stringpredicates where db_name='"+datasource+"'");
      val sqlDFNumeric = spark.sql("SELECT category FROM  "
                                     +metaDB+".numericpredicates where db_name='"+datasource+"'");
      val sqlDFtimeStamp = spark.sql("SELECT category FROM  "
                                     +metaDB+".timestamppredicates where db_name='"+datasource+"'");
      val sqlDFdate = spark.sql("SELECT category FROM  "
                                     +metaDB+".datepredicates where db_name='"+datasource+"'");
      val sqlDFBoolean = spark.sql("SELECT category FROM  "
                                     +metaDB+".booleanpredicates where db_name='"+datasource+"'");
      val sqlDFbinary = spark.sql("SELECT category FROM  "
                                     +metaDB+".binarypredicates where db_name='"+datasource+"'");
      val hashMap = new java.util.HashMap[String,Object]()
      import spark.sqlContext.implicits._
      sqlDFString.columns.map(col => {
         hashMap.put("string" ,
         sqlDFString.groupBy(col)
           .count()
           .map(_.getString(0))
           .collect())
         })
      sqlDFNumeric.columns.map(col => {
         hashMap.put("numerics" ,
         sqlDFNumeric.groupBy(col)
           .count()
           .map(_.getString(0))
           .collect())
         })
      sqlDFtimeStamp.columns.map(col => {
         hashMap.put("timestamp" ,
         sqlDFtimeStamp.groupBy(col)
           .count()
           .map(_.getString(0))
           .collect())
         })
      sqlDFdate.columns.map(col => {
         hashMap.put("date" ,
         sqlDFdate.groupBy(col)
           .count()
           .map(_.getString(0))
           .collect())
         })
       sqlDFBoolean.columns.map(col => {
         hashMap.put("boolean" ,
         sqlDFBoolean.groupBy(col)
           .count()
           .map(_.getString(0))
           .collect())
         })
       sqlDFbinary.columns.map(col => {
         hashMap.put("binary" ,
         sqlDFbinary.groupBy(col)
           .count()
           .map(_.getString(0))
           .collect())
         })
      javaHashMap.put("data", hashMap)
      javaHashMap.put("msg", "Success")
      println("Successfull  createDataSourceInfo ")    
      javaHashMap
    }catch{
      case e:Exception => {javaHashMap.put("msg", "Error")       
        javaHashMap.put("error", e.toString()) 
        println("UnSuccessfull  createDataSourceInfo ")    
      javaHashMap}
    } 
  }
  def getSubCategories(map: java.util.HashMap[String,Object]): java.util.HashMap[String,Object] ={
     val javaHashMap = new java.util.HashMap[String,Object]() 
     try{
       val mainDB = config.getString("databases.maindb")
       var messageExecuter = new MessageExecuter()
       val spark : SparkSession = messageExecuter.spark
       val dsName = map.get("dsName").toString();
       val selCat = map.get("predicate").toString();
       import spark.implicits._
       var dfMap = StaticDataFrames.DATAFRAMES
       val dsDf = dfMap(dsName)
       //val hiveresult = spark.sql("SELECT DISTINCT "+selCat+" FROM "+mainDB+"."+dsName);
       var hashMap = new java.util.HashMap[String,Array[Any]]() 
       
       import scala.reflect.ClassTag
       implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)  
       var arr =dsDf.groupBy(selCat)
                             .count()
                             .map(_.get(0))
                             .collect()
       hashMap.put(selCat, arr.distinct)
      /*hiveresult.schema.map { col => {
        hashMap.put(col.name , hiveresult.groupBy(col.name)
                                       .count()
                                       .map(_.get(0))
                                       .collect())
         
       } }*/
       javaHashMap.put("data", hashMap)
       javaHashMap.put("msg", "Success")
       javaHashMap
     }catch{
       case e:Exception => {
         println(e.toString())
         javaHashMap.put("msg", "Error")       
         javaHashMap.put("error", e.toString())    
         javaHashMap
         }
     }
  }
  def getMaxMin(map: java.util.HashMap[String,Object]):java.util.HashMap[String,Object] = {
    val javaHashMap = new java.util.HashMap[String,Object]() 
     try{
       val mainDB = config.getString("databases.maindb")
       var messageExecuter = new MessageExecuter()
       val spark : SparkSession = messageExecuter.spark
       val dsName = map.get("dsName").toString();
       val selCat = map.get("predicate").toString();
       import spark.implicits._
       var dfMap = StaticDataFrames.DATAFRAMES
       val dsDf = dfMap(dsName)
      // val dsDf = spark.sql("SELECT DISTINCT "+selCat+" FROM "+mainDB+"."+dsName);
       var hashMap = new java.util.HashMap[String,Array[Any]]() 
       
       import scala.reflect.ClassTag
       implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)  
       var maxDf = dsDf.select(max(selCat))
       var minDf = dsDf.select(min(selCat))
       maxDf.columns.map { col => {
         var maxarr = maxDf.groupBy(col)
             .count()
             .map(_.get(0))
             .collect()
         var maxValue = maxarr(0)
         javaHashMap.put("max" ,maxValue.asInstanceOf[Object])
       } }
       minDf.columns.map { col => {
         var minarr = minDf.groupBy(col)
             .count()
             .map(_.get(0))
             .collect()
         var minValue = minarr(0)
         javaHashMap.put("min" ,minValue.asInstanceOf[Object])
       } }
       javaHashMap.put("data", hashMap)
       javaHashMap.put("msg", "Success")
       javaHashMap
     }catch{
       case e:Exception => {
         println(e.toString())
         javaHashMap.put("msg", "Error")       
         javaHashMap.put("error", e.toString())    
         javaHashMap
         }
     }
  }
  def getDataFrame(map: java.util.HashMap[String,Object]):java.util.HashMap[String,Object] = {    
    val javaHashMap = new java.util.HashMap[String,Object]() 
    try{
      val dsName = map.get("dsName").toString();
      val mainDB = config.getString("databases.maindb")
       var messageExecuter = new MessageExecuter()
       val spark : SparkSession = messageExecuter.spark
       import spark.implicits._
       var dfMap = StaticDataFrames.DATAFRAMES
       //var dsDf = dfMap(dsName)
       val dsDf = spark.sql("SELECT * from "+mainDB+"."+dsName);
       javaHashMap.put("msg", "success")
       javaHashMap.put("df", dsDf)      
    }catch{
       case e:Exception => {
         println(e.toString())
         javaHashMap.put("msg", "Error")       
         javaHashMap.put("error", e.toString())
         }
     }
    javaHashMap    
  }
  def getTableData(map:java.util.HashMap[String,java.util.ArrayList[java.util.HashMap[String,Object]]],dsName:String):Future[java.util.HashMap[String,Object]]=Future{
    println("getTableData invoked")
    val mainDB = config.getString("databases.maindb")
    var messageExecuter = new MessageExecuter()
    val spark : SparkSession = messageExecuter.spark
    val javaHashMap = new java.util.HashMap[String,Object]() 
    val inputMap = map.asScala;
    var queryString = "SELECT * FROM "+mainDB+"."+dsName+" WHERE "
    var condition = new ArrayBuffer[String]();
    try{
      var whereString = "";
      inputMap.map(cat=>{
        val catMap = cat._2.asScala;
        if(cat._1.equals("numeric")|| cat._1.equals("date") || cat._1.equals("timestamp")){          
          catMap.map(sCatName=>{
            val sCatMap = sCatName.asScala;
            sCatMap.map(sCat =>{
              var arr = sCat._2.asInstanceOf[java.util.ArrayList[Object]]
              whereString = whereString+sCat._1+">="+arr.get(0).toString()+" AND "+sCat._1+"<="+arr.get(1).toString()+" ";
            });            
          })
          condition.+=(whereString)
        }else{            
            catMap.map(sCatName=>{
              var sCatMap = sCatName.asScala;
              sCatMap.map(sCat =>{
                  var arr = sCat._2.asInstanceOf[java.util.ArrayList[Object]]
                  var i =0 ;
                  var whereString = "";
                  println(arr.size())
                  if(arr.size() == 1){
                    whereString = whereString+sCat._1+"='"+arr.get(i)+"'";
                  }else{
                    while(i<arr.size()){
                      if(i == arr.size()-1){
                        whereString = whereString+"'"+arr.get(i)+"' ) ";
                      }else if(i == 0){
                        whereString = whereString+sCat._1+" IN ( '"+arr.get(i)+"', ";
                      }else{
                        whereString = whereString+"'"+arr.get(i)+"',";
                      }
                      i = i+1;
                    }  
                  }            
                  condition.+=(whereString);
              })          
          })          
        }        
      })
      var i = 0;
      while(i<condition.length){
        if(i == condition.length-1){
          queryString = queryString+condition(i);
        }else{
          queryString = queryString+condition(i)+" AND "
        }
        i=i+1
      }
      println("*******************************************************")
      println(queryString)
      println("*******************************************************************")
      val dsDf = spark.sql(queryString);
      val hashMap = new java.util.HashMap[String,Object]()
      var colNames = dsDf.columns;
//      import scala.reflect.ClassTag
//      implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)  
//      dsDf.columns.map(col => {
//         hashMap.put(col ,
//         dsDf.groupBy(col)
//           .count()
//           .map(_.get(0))
//           .collect())
//         })
      //println(write(hashMap.asScala))
      var javaRDD = (dsDf.repartition(4)).toJavaRDD
           
      javaHashMap.put("msg", "success")
      javaHashMap.put("data", javaRDD)
      javaHashMap.put("colNames", colNames)
      
    }catch{
       case e:Exception => {
         println(e.toString())
         javaHashMap.put("msg", "Error")       
         javaHashMap.put("error", e.toString())
         }
     }
    javaHashMap
  }
}