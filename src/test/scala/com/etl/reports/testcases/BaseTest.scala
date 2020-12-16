package com.etl.reports.testcases

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class BaseTest extends FunSuite with BeforeAndAfter with Matchers{

  var sparkSession:SparkSession = _

  before {
    sparkSession = SparkSession.builder()
      .master("local")
      .appName("click-stream")
      .enableHiveSupport()
      .getOrCreate()
  }
}
