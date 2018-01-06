package com.pridictit.hive

import org.apache.spark.sql.DataFrame
import scala.collection.immutable.HashMap
import collection.JavaConverters._
import com.pridictit.Server.MessageExecuter
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object StaticDataFrames{
  var DATAFRAMES :Map[String,DataFrame] = new HashMap[String,DataFrame]()  
  private var METADATA :Map[String,DataFrame] = new HashMap[String,DataFrame]()
}
class StaticDataFrames {
  
  def fillMainDataFrames():java.util.HashMap[String,Object] = {
    var javaHashMap = new java.util.HashMap[String,Object]() 
   try{
      //var dF = StaticDataFrames.DATAFRAMES
      var gd = new GetData()
      val config = ConfigFactory.load()
      var datasources1 = gd.getDataSources()
      var datasources = datasources1.asScala
      val mainDB = config.getString("databases.maindb")
      var messageExecuter = new MessageExecuter()
      val spark : SparkSession = messageExecuter.spark
      if(datasources("msg").equals("Success")){
        var data = datasources("data").asInstanceOf[java.util.HashMap[String,Object]]
        var dataMap = data.asScala
        dataMap.map(f=>{          
          var arr = f._2.asInstanceOf[Array[String]]
          println(arr)
          arr.map { ds => {
            println(ds)
           // import spark.implicits._
            val hiveresult = spark.sql("SELECT * FROM "+mainDB+"."+ds);
            StaticDataFrames.DATAFRAMES += (ds -> hiveresult)
          } }
        });
      }else{
        println("errrorrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr")
      }
     
      javaHashMap.put("msg", "Success")
      javaHashMap
    }catch{
      case e :Exception =>{
        javaHashMap.put("msg", "Failure")
        javaHashMap
        //println("Filling dataframes is unsuccessfull......")
      }
    }
  }
  def fillMetaDataFrames(){
    var javaHashMap = new java.util.HashMap[String,Object]() 
    try{
      
      javaHashMap.put("msg", "Success")
      javaHashMap
    }catch{
      case e :Exception =>{
        javaHashMap.put("msg", "Failure")
        javaHashMap
        println("Filling dataframes is unsuccessfull......")
      }
    }
  }
  def getDataFrame(dsName:String){
    var javaHashMap = new java.util.HashMap[String,Object]();
    try{
      val df = StaticDataFrames.DATAFRAMES(dsName); 
      javaHashMap.put("msg", "Success")
      javaHashMap.put("df", df)
      javaHashMap
    }catch{
      case e :Exception =>{
        javaHashMap.put("msg", "Failure")
        javaHashMap
        println("Filling dataframes is unsuccessfull......")
      }
    }
  }
  
}