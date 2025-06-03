package com.xueyingying.datalake.e2e.flink

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import com.xueyingying.datalake.FlinkSuiteBase
import org.apache.flink.cep.nfa.compiler.NFACompiler
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition

import scala.util.{Failure, Success, Using}

/**
 * @author cx330.1000ly@gmail.com
 * @version 1.0.0
 * @since 2023-01-10
 */
class FlinkCEPTest extends FlinkSuiteBase {
  "a" should "pass" in {
    val pattern: Pattern[String, String] = Pattern.begin("first")
      .where(new SimpleCondition[String]() {
        override def filter(value: String): Boolean = true
      })
      .next("second")
      .where(new SimpleCondition[String]() {
        override def filter(value: String): Boolean = true
      })
      .next("third")
      .where(new SimpleCondition[String]() {
        override def filter(value: String): Boolean = true
      })

    val nfaFactory: NFACompiler.NFAFactory[String] = NFACompiler.compileFactory(pattern, false)

    val value = Using(new ByteArrayOutputStream()) { bos =>
      Using(new ObjectOutputStream(bos)) { oos =>
        oos.writeObject(nfaFactory)
      }
      bos.toByteArray
    } match {
      case Success(byteArray) =>
        Base64.getEncoder.encodeToString(byteArray)
      case Failure(e) => throw e
    }

    val bytes = Base64.getDecoder.decode(value)
    Using(new ByteArrayInputStream(bytes)) { bis =>
      Using(new ObjectInputStream(bis)) { ois =>
        val a = ois.readObject().asInstanceOf[NFACompiler.NFAFactory[String]]
        println(a)
      }
    }
  }
}
