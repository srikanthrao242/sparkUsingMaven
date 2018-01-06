package com.pridictit.main

import java.sql.Connection

import scala.collection.mutable.HashMap
import collection.JavaConverters._
import scala.collection.Map

class UserInfor
{

  def insertUserData(userdata : Map[String,Any]) : java.util.HashMap[String,Object] =
  {
    var result  =new java.util.HashMap[String,Object]
    val conn = new LocalDatabase()
    var connection: Connection = null

    val emailid: String = userdata.get("email_id").map(_.toString).getOrElse("")
    val password: String = userdata.get("password").map(_.toString).getOrElse("")
    val queryString: String = "insert into user_info(email_id,password) values ('" + emailid + "','" + password + "');"

    try
    {
      connection = conn.newConnection()
      val statement = connection.createStatement()
      if(this.checkUserEmail(emailid,connection) == "yes")
      {
        val resultSet = statement.executeUpdate(queryString)
        result.put("message", "Success")
        result.put("data", "user added successfully")
       // result += ("message" -> "success")
       // result += ("data" -> "user added successfully")
      }
      else
      {
        result.put("message", "Success")
        result.put("data", "user already exist")
        //result += ("message" -> "success")
       // result += ("data" -> "user already exist")
      }

    }
    catch {
      case e: Throwable => e.printStackTrace()
    }
    finally
    {
      connection.close()
    }

    result
  }

  def checkUserEmail(emailid: String, connection : Connection): String =
  {

    val statement = connection.createStatement()
    val queryString: String = "SELECT * FROM user_info WHERE email_id = '"+emailid+"';"
    val resultSet = statement.executeQuery(queryString)
    if(!resultSet.next())
    {
      "yes"
    }
    else
    {
      "no"
    }

  }


}

