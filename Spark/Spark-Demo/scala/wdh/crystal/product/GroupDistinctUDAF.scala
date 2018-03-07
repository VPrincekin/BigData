package com.aura.meituan.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by Administrator on 2017/6/27.
  */
object GroupDistinctUDAF extends UserDefinedAggregateFunction{
  /**
    * 输入的数据类型  1:beijing
    * @return
    */
  override def inputSchema: StructType =StructType(
    StructField("city_info",StringType,true)::Nil
  )

  /**
    *定义输出的类型
    * @return
    */
  override def dataType: DataType = StringType


  /**
    * 定义辅助字段
    * @return
    */
  override def bufferSchema: StructType = {
    bufferSchema.add("buffer_city_info",StringType,true)
  }


  /**
    * 初始化辅助字段
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer.update(0,"")
  }

  /**
    * 局部操作
    * @param buffer 上一次缓存的结果
    * @param input  input 这一次的输入
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var buffer_str = buffer.getString(0)   //""
    val current_str = input.getString(0)  //1:beijing    1:beijing,2:shenzheng
    if(!buffer_str.contains(current_str)){
       if(buffer_str.eq("")){
         buffer_str=current_str
       }else{
         buffer_str+","+current_str
       }
    }
    buffer.update(0,buffer_str)
  }

  /**
    * 进行全局汇总
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
     var b1 = buffer1.getString(0)  //1:beijing,2:shenzheng
     val b2 = buffer2.getString(0)  //3:shanghai,2:shenzheng   1:beijing,2:shenzheng,3:shanghai
    for(b <- b2.split(",")){
      if(!b1.contains(b)){
        if(b1.equals("")){
          b1=b
        }else{
          b1+","+b
        }

      }
    }
    b1.updated(0,b1)
    // 1:beijing,2:shenzheng,3:shanghai

  }

  /**
    * 拿到最后的结果
    * @param buffer
    * @return
    */

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }



  override def deterministic: Boolean = true

}
