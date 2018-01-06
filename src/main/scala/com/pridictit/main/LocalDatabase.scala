package com.pridictit.main


import java.sql.Connection._
import java.sql.{Connection, Driver, DriverManager}

import com.typesafe.config.ConfigFactory

import scala.collection.immutable.Vector

class LocalDatabase
{
  
  val config = ConfigFactory.load();
  val password = config.getString("localmysql.password");
  val url = config.getString("localmysql.url");
  val username = config.getString("localmysql.user");
  val driver = config.getString("localmysql.driverName");
  
  def newConnection() : Connection = synchronized
  {
    var connection : Connection = null;

    try
    {
      Class.forName(driver);

      connection = DriverManager.getConnection(url, username, password);

    }
    catch
      {
        case e: Throwable => e.printStackTrace()
      }
    connection
  }

}
