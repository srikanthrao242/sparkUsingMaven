package com.pridictit.hive

import java.io.{BufferedWriter, FileWriter, File, PrintWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.HashMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._
import scala.collection.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import scala.util.Success
import scala.util.Failure
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import org.apache.spark.sql._
import scala.concurrent.ExecutionContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}
import com.pridictit.Server.MessageExecuter

class Parquet 
{
  val config = ConfigFactory.load()
  
  def getDataFrame(csvFile : String,delimiter : String): Map[String,Any] = {
    
    var result :Map[String,Any] = new HashMap[String,Any]()
    
    try{
         var messageExecuter = new MessageExecuter()
    
         val dataframe= messageExecuter.spark.read
                        .format("com.databricks.spark.csv")
                        .option("header", "true") // Use first line of all files as header
                        .option("inferSchema", "true")
                        .option("delimiter",delimiter)
                        .load(csvFile)               
          result +=("msg"->"Success")
          result +=("df"->dataframe)          
          result +=("function"->"getDataFrame")          
          result
    }catch{
      case e :Exception => {result +=("msg"->"Error")          
                              result +=("df"->null)                              
                              result +=("function"->"getDataFrame")                              
                              result +=("error"->e.toString())                              
                              result
                              }
    }
  }
  def getSchemaMap (dataframe : DataFrame) : Map[String,Any] =
  {
    var result :Map[String,Any] = new HashMap[String,Any]()
    try
    {
      var dfMap = dataframe.schema    
      val javaHashMap = new java.util.HashMap[String,Object]()      
      dfMap.fields.map { x => javaHashMap.put(x.name,x.dataType.simpleString.toUpperCase()) }      
      result +=("msg"->"Success")          
      result +=("javaHashMap"->javaHashMap)      
      result +=("function"->"getSchemaMap")
      result
      
    }
    catch
    {
      case e : Exception => {result +=("msg"->"Error")          
                              result +=("javaHashMap"->null)                              
                              result +=("function"->"getSchemaMap")                              
                              result +=("error"->e.toString())                              
                              result        
      }
    }   
  }
  def saveDataFrameAsParquet(dataframe : DataFrame,tablename:String): Map[String,Any] = {    
    var result :Map[String,Any] = new HashMap[String,Any]()
    val mainDB = config.getString("databases.maindb")
    try{
       var messageExecuter = new MessageExecuter()                  
       dataframe.write.format("parquet").saveAsTable(mainDB+"."+tablename)       
       result +=("msg"->"Success")       
       result +=("function"->"saveDataFrameAsParquet")       
       result      
    }catch{
      case e:Exception => {result +=("msg"->"Error")                              
                              result +=("function"->"saveDataFrameAsParquet")                              
                              result +=("error"->e.toString())                              
                              result}
    }
  }
  def createDataSourceInfo() : java.util.HashMap[String,Object] =
  {
    val javaHashMap = new java.util.HashMap[String,Object]()
    val metaDB = config.getString("databases.metadb")
    try{      
       var messageExecuter = new MessageExecuter()
       val spark : SparkSession = messageExecuter.spark
       val dataframe = spark.sql("CREATE TABLE IF NOT EXISTS "
                                     +metaDB+".database_info ( db_id int, db_name String, "
                                     +" user_id int)");
      
      javaHashMap.put("msg", "Success")               
      javaHashMap.put("function", "createDataSourceInfo") 
      println("Successfull  createDataSourceInfo ")    
      javaHashMap 
    }catch{
      case e:Exception => {javaHashMap.put("msg", "Error")               
        javaHashMap.put("function", "createDataSourceInfo")        
        javaHashMap.put("error", e.toString()) 
        println("UnSuccessfull  createDataSourceInfo ")    
      javaHashMap}
    } 
    
  }
  def insertPredicates(data : java.util.HashMap[String,Object],tableName: String) : java.util.HashMap[String,Object] = {
    val javaHashMap = new java.util.HashMap[String,Object]()
    val metaDB = config.getString("databases.metadb")
    try{
      var messageExecuter = new MessageExecuter()
      val spark : SparkSession = messageExecuter.spark
      var data1 = data.asScala
      var stringMap :Map[String,Any] = new HashMap[String,Any]()
      data1.map(f=>{
        if(f._2.toString().toLowerCase().contains("int") 
            ||f._2.toString().toLowerCase().equals("float") 
            ||f._2.toString().toLowerCase().equals("double")
            ||f._2.toString().toLowerCase().equals("decimal")
            ||f._2.toString().toLowerCase().contains("tinyint")
            ||f._2.toString().toLowerCase().contains("smallint")
            ||f._2.toString().toLowerCase().contains("bigint")){
          var queryString = "INSERT INTO TABLE "+metaDB+".numericpredicates  "
          queryString = queryString+"VALUES('"+tableName+"','"+f._1.toString()+"','"+f._2.toString()+"')"
          var res = spark.sql(queryString) 
        }else if(f._2.toString().toLowerCase().equals("timestamp")){
          var queryString = "INSERT INTO TABLE "+metaDB+".timestamppredicates  "
          queryString = queryString+"VALUES('"+tableName+"','"+f._1.toString()+"','"+f._2.toString()+"')"
          var res = spark.sql(queryString)
        }else if(f._2.toString().toLowerCase().equals("date")){
          var queryString = "INSERT INTO TABLE "+metaDB+".datepredicates  "
          queryString = queryString+"VALUES('"+tableName+"','"+f._1.toString()+"','"+f._2.toString()+"')"
          var res = spark.sql(queryString)
        }else if(f._2.toString().toLowerCase().equals("boolean")){
          var queryString = "INSERT INTO TABLE "+metaDB+".booleanpredicates  "
          queryString = queryString+"VALUES('"+tableName+"','"+f._1.toString()+"','"+f._2.toString()+"')"
          var res = spark.sql(queryString)
        }else if(f._2.toString().toLowerCase().equals("binary")){
          var queryString = "INSERT INTO TABLE "+metaDB+".binarypredicates  "
          queryString = queryString+"VALUES('"+tableName+"','"+f._1.toString()+"','"+f._2.toString()+"')"
          var res = spark.sql(queryString)
        }else{
          var queryString = "INSERT INTO TABLE "+metaDB+".stringpredicates  "
          queryString = queryString+"VALUES('"+tableName+"','"+f._1.toString()+"','"+f._2.toString()+"')"
          var res = spark.sql(queryString)
        }
      })
      javaHashMap.put("msg", "Success")               
      javaHashMap
    }catch{
      case e:Exception => {javaHashMap.put("msg", "Error")       
        javaHashMap.put("error", e.toString()) 
        println("UnSuccessfull  createDataSourceInfo ")    
      javaHashMap}
    }
  }
  def dropTable(data:java.util.HashMap[String,Object]):Future[java.util.HashMap[String,Object]] = Future{
    var messageExecuter = new MessageExecuter()
    val metaDB = config.getString("databases.metadb")
    val mainDB = config.getString("databases.maindb")
    val javaHashMap = new java.util.HashMap[String,Object]()
    try{
      val spark = messageExecuter.spark
      val db_name = data.get("db_name").toString()  
      var queryString = "DELETE FROM metadb.database_info WHERE db_name="+db_name;
      spark.sql(queryString) 
      
      var intQuery_str = "DELETE FROM "+metaDB+".numericpredicates db_name="+db_name;
      spark.sql(queryString) 
      
      var timeQuery_str = "DELETE FROM "+metaDB+".timestamppredicates db_name="+db_name;
      spark.sql(queryString) 
      
      var dateQuery_str = "DELETE FROM "+metaDB+".datepredicates db_name="+db_name;
      spark.sql(queryString) 
      
      var boolQuery_str = "DELETE FROM "+metaDB+".booleanpredicates db_name="+db_name;
      spark.sql(queryString) 
      
      var binaryQuery_str = "DELETE FROM "+metaDB+".binarypredicates db_name="+db_name;
      spark.sql(queryString) 
      
      var strQuery_str = "DELETE FROM "+metaDB+".stringpredicates db_name="+db_name;
      spark.sql(queryString) 
      
      var dropTable_str = "DROP TABLE IF EXISTS "+mainDB+"."+db_name;
      spark.sql(queryString) 
      
      javaHashMap.put("msg", "Success")               
      javaHashMap
      
    }catch{
      case e:Exception => {javaHashMap.put("msg", "Error")               
        javaHashMap.put("function", "insertDatasource_info")        
        javaHashMap.put("error", e.toString()) 
        println("UnSuccessfull  insertDatasource_info ")    
        javaHashMap}
    } 
  }
  def insertDatasource_info(data : java.util.HashMap[String,Object]) : java.util.HashMap[String,Object] =
  {
    var messageExecuter = new MessageExecuter()
    val javaHashMap = new java.util.HashMap[String,Object]()
    try{
          var queryString = "INSERT INTO TABLE metadb.database_info  "
          val db_id = data.get("db_id").asInstanceOf[Int]
          val db_name = data.get("db_name").toString()
          val user_id = data.get("user_id").asInstanceOf[Int]          
          queryString = queryString+"VALUES("+db_id+",'"+db_name+"',"+user_id+")"          
          val spark = messageExecuter.spark          
          var res = spark.sql(queryString)          
          javaHashMap.put("msg", "Success")               
          javaHashMap.put("function", "insertDatasource_info")     
          println("Successfull  insertDatasource_info ")        
          javaHashMap 
    }catch{
      case e:Exception => {javaHashMap.put("msg", "Error")               
        javaHashMap.put("function", "insertDatasource_info")        
        javaHashMap.put("error", e.toString()) 
        println("UnSuccessfull  insertDatasource_info ")    
        javaHashMap}
    } 
  }
  def createPredicatesTables():java.util.HashMap[String,Object]={
    val javaHashMap = new java.util.HashMap[String,Object]()
    val metaDB = config.getString("databases.metadb")
    try{
      var messageExecuter = new MessageExecuter() 
      val sc : SparkContext = messageExecuter.sc      
      val spark : SparkSession = messageExecuter.spark 
      val stringDataframe = spark.sql("CREATE TABLE IF NOT EXISTS "
                                     +metaDB+".stringpredicates ( db_name String, "
                                     +"category String ,"
                                     +"type String )");
      val numericDataframe = spark.sql("CREATE TABLE IF NOT EXISTS "
                                     +metaDB+".numericpredicates ( db_name String, "
                                     +"category String ,"
                                     +"type String )");
      val timeStampDataframe = spark.sql("CREATE TABLE IF NOT EXISTS "
                                     +metaDB+".timestamppredicates ( db_name String, "
                                     +"category String ,"
                                     +"type String )");
      val dateDataframe = spark.sql("CREATE TABLE IF NOT EXISTS "
                                     +metaDB+".datepredicates ( db_name String, "
                                     +"category String ,"
                                     +"type String )");
      val booleanDataframe = spark.sql("CREATE TABLE IF NOT EXISTS "
                                     +metaDB+".booleanpredicates ( db_name String, "
                                     +"category String ,"
                                     +"type String )");
      val binaryDataframe = spark.sql("CREATE TABLE IF NOT EXISTS "
                                     +metaDB+".binarypredicates ( db_name String, "
                                     +"category String ,"
                                     +"type String )");
      javaHashMap.put("msg", "Success")               
      javaHashMap.put("function", "createDataSourceInfo") 
      println("Successfull  createDataSourceInfo ")    
      javaHashMap
    }catch{
      case e:Exception =>{
        javaHashMap.put("msg", "Error")      
        javaHashMap.put("error", e.toString()) 
        println("UnSuccessfull  insertDatasource_info ")    
        javaHashMap
      }
    }
  }
  def createMCTable(tableMap :Map[String,Any],tableName: String) :  Map[String,Any] =
  {
    var messageExecuter = new MessageExecuter()    
    var result :Map[String,Any] = new HashMap[String,Any]()    
    val sc : SparkContext = messageExecuter.sc      
    val spark : SparkSession = messageExecuter.spark    
    var tableSchema  = tableMap    
    var tablename = "MC_"+tableName    
    val metaDB = config.getString("databases.metadb")    
    try
    {           
      var tablesch = spark.sparkContext.makeRDD(tableSchema.toSeq)  
      val dfRdd = tablesch.map { 
            case (s0, s1) => Row(s0, s1.toString()) }      
      val schema = StructType(StructField("cat_Name", StringType, true)::
                          StructField("dataType", StringType, true)::Nil)                            
      val df1 =  spark.createDataFrame(dfRdd, schema)      
      import spark.sqlContext.implicits._                      
      df1.write.format("parquet").saveAsTable(metaDB+"."+tablename)      
      println("categpries and measures are added")      
      result +=("msg"->"Success")       
      result +=("function"->"createMCTable")     
      result           
    }
    catch
    {
      case e : Exception => {
                              result +=("msg"->"Error")                              
                              result +=("function"->"createMCTable")                              
                              result +=("error"->e.toString())                              
                              result
      }
    }    
  }
  def createDatabases() :java.util.HashMap[String,Object]=
  {
    val javaHashMap = new java.util.HashMap[String,Object]()
    try
    {
      var messageExecuter = new MessageExecuter()     
      val sc : SparkContext = messageExecuter.sc
      val spark : SparkSession = messageExecuter.spark      
      val mainDB = config.getString("databases.maindb")      
      val metaDB = config.getString("databases.metadb")      
      val databasesQuery = "CREATE DATABASE IF NOT EXISTS "+mainDB      
      val metaDBQuery = "CREATE DATABASE IF NOT EXISTS "+metaDB      
      spark.sql(databasesQuery)      
      spark.sql(metaDBQuery)      
      javaHashMap.put("msg", "Success")               
      javaHashMap.put("function", "createDatabases") 
      println("Successfull  databases ")    
      javaHashMap       
    }
    catch
    {
      case e : Exception =>{
        javaHashMap.put("msg", "Error")               
        javaHashMap.put("function", "createDatabases")        
        javaHashMap.put("error", e.toString()) 
             println("UnSuccessfull  databases ")    
      javaHashMap 
      }
    }
  }
  def createTable (tableMap : java.util.HashMap[String,Object]):Future[java.util.HashMap[String,Object]] =Future   
  {
    println("************************************************************************************************************")
    
    val javaHashMap = new java.util.HashMap[String,Object]()    
    var messageExecuter = new MessageExecuter()     
    val sc : SparkContext = messageExecuter.sc      
    val spark : SparkSession = messageExecuter.spark    
    var csvFile : String = tableMap.get("csvFile").toString()     
    var tablename = tableMap.get("tablename").toString()    
    var tableName = tablename    
    var delimiter = tableMap.get("delimiter").toString()    
    try
    {      
      val dataFrameMap = this.getDataFrame(csvFile,delimiter)      
      if(dataFrameMap("msg").toString().equals("Success"))
      {
        val dataframe = dataFrameMap("df").asInstanceOf[DataFrame]
        
        val javaMap = this.getSchemaMap(dataframe)
        
        if(javaMap("msg").toString().equals("Success"))
        {
          val javaHash = javaMap("javaHashMap").asInstanceOf[java.util.HashMap[String,Object]]
          
          val saveMC = this.insertPredicates(javaHash, tableName)
          
          if(saveMC.get("msg").toString().equals("Success"))
          {
            val savePrquet = this.saveDataFrameAsParquet(dataframe, tablename)            
            if(savePrquet("msg").toString().equals("Success"))
            {
               javaHashMap.put("msg", "Success")               
               javaHashMap.put("function", "createTable")   
               println("Successfull  createTable ")              
               javaHashMap 
            }else{
              javaHashMap.put("msg", "Error")              
              javaHashMap.put("error", savePrquet("error").toString())              
              javaHashMap.put("function", "saveDataFrameAsParquet")   
               println("UnSuccessfull  createTable ")              
               javaHashMap 
            }
          }
          else
          {
             javaHashMap.put("msg", "Error")
             javaHashMap.put("error", saveMC.get("error").toString())
             println("UnSuccessfull  createTable ")            
             javaHashMap 
          }
        }
        else
        {
           javaHashMap.put("msg", "Error")           
           javaHashMap.put("error", javaMap("error").toString())           
           javaHashMap.put("function", javaMap("function").toString()) 
           println("UnSuccessfull  createTable ")          
           javaHashMap 
        }
      }
      else
      {
         javaHashMap.put("msg", "Error")         
         javaHashMap.put("error", dataFrameMap("error").toString())         
         javaHashMap.put("function", dataFrameMap("function").toString()) 
         println("Successfull  createTable ")        
         javaHashMap 
      }
      
    }
    catch
    {
      case e : Exception => {
        
        println("Exception in createTable ")        
        javaHashMap.put("msg", "Error")        
        javaHashMap.put("function", "createTable")        
        javaHashMap.put("error", e.toString())        
        javaHashMap
      }
    }
  }
  
  def getSchema(csv : Map[String, Any]): java.util.HashMap[String,Object] = 
  {
    var result : Map[String, Any] = new HashMap[String, Any]    
    val javaHashMap = new java.util.HashMap[String,Object]()    
    val javadatatypes = new java.util.HashMap[String,Object]()    
    var datatypes : Map[String, String] = new HashMap[String, String]    
    var messageExecuter = new MessageExecuter()     
    val sc : SparkContext = messageExecuter.sc      
    val spark : SparkSession = messageExecuter.spark    
    var csvFile : String = csv("csvFile").toString()     
    //var delimiter = csv("delimiter").toString()    
    try
    {        
       val dataframe= spark.read
                  .format("com.databricks.spark.csv")
                  .option("header", "true") // Use first line of all files as header
                  .option("inferSchema", "true")
                  .option("delimiter",",")
                  .load(csvFile)
                  
        val javadatatypes = new java.util.HashMap[String,Object]()        
        dataframe.schema.fields.map { x => javadatatypes.put(x.name,x.dataType.simpleString.toUpperCase()) }       
        javaHashMap.put("message","Success")        
        javaHashMap.put("data",javadatatypes)        
        javaHashMap.put("DataFrame", dataframe)        
        javaHashMap
        
    }
    catch
    {
      case e :Throwable =>
      {
        println("Exception in getSchema function"+e)        
        javaHashMap.put("message","Error")        
        javaHashMap.put("data",e.toString())
      }        
        javaHashMap       
    } 
  }  
  
  def getData (): java.util.HashMap[String,Object] =
  {
     val javaHashMap = new java.util.HashMap[String,Object]()     
     var messageExecuter = new MessageExecuter()     
     val sc : SparkContext = messageExecuter.sc      
     val spark : SparkSession = messageExecuter.spark     
    // val queryString : String = getData("queryString").toString()     
     try
     {//"SELECT * FROM parquet.`examples/src/main/resources/users.parquet`"
       var sqlres = spark.sql("select * from metadb.mc_allmonths")       
       sqlres.show()       
       val queryresult = new java.util.HashMap[String,Object]()
       
       javaHashMap.put("message", "Success")
     }
     catch
     {
       case e : Exception =>  javaHashMap.put("message", "Error")
     }
     
     javaHashMap
     
  }
}