package com.asiainfo.ocdc.streaming.tool

import scala.reflect.ClassTag
import java.nio.ByteBuffer
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.Kryo
import org.objenesis.strategy.StdInstantiatorStrategy

/**
 * Created by baishuo on 3/31/15.
 */
class KryoSerializerStreamAppTool {
  private val bufferSize: Int = (0.064 * 1024 * 1024).toInt
  private val maxBufferSize = 64 * 1024 * 1024
  private val kryo = new Kryo()
  kryo.setInstantiatorStrategy(new StdInstantiatorStrategy)
  private lazy val output = new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))
  private lazy val input = new KryoInput()

  def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    kryo.writeClassAndObject(output, t)
    ByteBuffer.wrap(output.toBytes)
  }

  def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    input.setBuffer(bytes.array)
    kryo.readClassAndObject(input).asInstanceOf[T]
  }

  def deserializeWithType(bytes: ByteBuffer) = {
    input.setBuffer(bytes.array)
    val reg = kryo.readClass(input)
    val s = kryo.readObject(input, reg.getType())
    (s, reg.getType)
  }

  def register(clazz: Class[_]) = {
    kryo.register(clazz)
  }

}

object Test {

  // 测试用
  def main(args: Array[String]) {
    val tool = new KryoSerializerStreamAppTool

    for (i <- 0 to 1000000000) {
      val student = new EnglishStudent("aa", "male", 25)
      val buffer = tool.serialize(student)
      val student2 = tool.
        deserialize[com.asiainfo.ocdc.streaming.tool.Student](buffer) // 好用
//      println(student2)
      val student3 = new EnglishStudent("aa", "male", 26)
      val buffer2 = tool.serialize(student)
      val (s, regType) = tool.deserializeWithType(buffer2)
//      println(s)
    }
  }

}

// 仅仅用来测试
class Student(val name: String, val sex: String, val age: Int) extends Serializable {

  override def toString: String = {
    name + "|" + sex + "|" + age;
  }
}

// 仅仅用来测试
class EnglishStudent(name: String, sex: String, age: Int) extends Student(name, sex, age) with Serializable {

  override def toString: String = {
    name + "|" + sex + "|" + age + "|english";
  }

  def aaa = {
    println("aaa")
  }
}
