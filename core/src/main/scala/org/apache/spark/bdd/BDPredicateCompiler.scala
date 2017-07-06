/** BDD START **/

package org.apache.spark.bdd

import java.io.File
import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile
import scala.tools.nsc.util.BatchSourceFile
import scala.tools.nsc.{Global, Settings}

object PredicateClassVersion {
	var count = 1
	val codelines = Array(7,8,2,1,0)
	val code_template = "def function(value:\n" +
		"/**Write input types for this function.\n" +
		"For Example : (String, Int) */\n\n\n\n\n" +
		"): Boolean = {\n" +
		"/**Write your function here**/\n\n\n\n\n\n\n" +
		"}\n"
	val codelinesst = Array(8,7,5,2,1,0)
	val code_template_st = "def function(value:\n" +
		"/**Write input types for this function below.\n" +
		"For Example : (String, Int) */\n\n\n" +
		"): /**Write return type here **/ \n\n" +
		"= {\n" +
		"/**Write your function body here**/\n\n\n" +
		"}\n"
}

class BDPredicateCompiler(targetDir: Option[File]) extends Logging {

	val file = new File("/tmp")
	if (!file.exists()) {
		file.mkdir()
	}
	val target = AbstractFile.getDirectory(file)
	val classCache = mutable.Map[String, Class[_]]()
	private val settings = new Settings()
	settings.deprecation.value = true // enable detailed deprecation warnings
	settings.unchecked.value = true // enable detailed unchecked warnings
	settings.outputDirs.setSingleOutput(target)
	settings.usejavacp.value = true

	private val global = new Global(settings)
	private lazy val run = new global.Run
	val classLoader = new AbstractFileClassLoader(target, this.getClass.getClassLoader)

	def compile(code: String) = {
		val className = classNameForCode(code)
		findClass(className).getOrElse {
			val sourceFiles = List(new BatchSourceFile("(inline)", wrapCodeInClass(className, code)))
			run.compileSources(sourceFiles)
			findClass(className).get
		}
	}

	def eval[T](code: String): T = {
		val cls = compile(code)
		cls.getConstructor().newInstance().asInstanceOf[T]
	}

	def findClass(className: String): Option[Class[_]] = {
		synchronized {
			classCache.get(className).orElse {
				try {
					val cls = classLoader.loadClass(className)
					classCache(className) = cls
					Some(cls)
				} catch {
					case e: ClassNotFoundException => None
				}
			}
		}
	}

	protected def classNameForCode(code: String): String = {
		val digest = MessageDigest.getInstance("SHA-1").digest(code.getBytes)
		PredicateClassVersion.count = PredicateClassVersion.count + 1
		"sha" + PredicateClassVersion.count + "b" + new BigInteger(1, digest).toString(16)
	}

	private def extractType(code: String): String = {
		val w = "For Example : (String, Int) */"
		val end = w.length + code.indexOf(w)
		val start = code.indexOf("): Boolean = {")
		code.substring(end, start).trim()
	}

	private def wrapCodeInClass(className: String, code: String) = {
		"class " + className + " extends org.apache.spark.bdd.BDDFilter[" + extractType(code) + "] {\n" +
			code +
			"}\n"
	}
}


class BDCodeFixCompiler(targetDir: Option[File]) extends Logging {

	val file = new File("/tmp")
	if (!file.exists()) {
		file.mkdir()
	}
	val target = AbstractFile.getDirectory(file)
	val classCache = mutable.Map[String, Class[_]]()
	private val settings = new Settings()
	settings.deprecation.value = true // enable detailed deprecation warnings
	settings.unchecked.value = true // enable detailed unchecked warnings
	settings.outputDirs.setSingleOutput(target)
	settings.usejavacp.value = true

	private val global = new Global(settings)
	private lazy val run = new global.Run
	val classLoader = new AbstractFileClassLoader(target, this.getClass.getClassLoader)

	def compile(code: String) = {
		val className = classNameForCode(code)
		findClass(className).getOrElse {
			val sourceFiles = List(new BatchSourceFile("(inline)", wrapCodeInClass(className, code)))
			run.compileSources(sourceFiles)
			findClass(className).get
		}
	}

	def eval[T](code: String): T = {
		val cls = compile(code)
		cls.getConstructor().newInstance().asInstanceOf[T]
	}

	def findClass(className: String): Option[Class[_]] = {
		synchronized {
			classCache.get(className).orElse {
				try {
					val cls = classLoader.loadClass(className)
					classCache(className) = cls
					Some(cls)
				} catch {
					case e: ClassNotFoundException => None
				}
			}
		}
	}

	protected def classNameForCode(code: String): String = {
		val digest = MessageDigest.getInstance("SHA-1").digest(code.getBytes)
		PredicateClassVersion.count = PredicateClassVersion.count + 1
		"sha" + PredicateClassVersion.count + "b" + new BigInteger(1, digest).toString(16)
	}

	private def extractReturnType(code: String): String = {
		val w = "): /**Write return type here **/"
		val end = w.length + code.indexOf(w)
		val start = code.indexOf("= {")
		code.substring(end, start).trim()
	}

	private def extractType(code: String): String = {
		val w = "For Example : (String, Int) */"
		val end = w.length + code.indexOf(w)
		val start = code.indexOf("): /**Write return type here **/ ")
		code.substring(end, start).trim()
	}

	private def wrapCodeInClass(className: String, code: String) = {
		val a = "class " + className + " extends org.apache.spark.bdd.BDCodeFix[" + extractType(code) + "," + extractReturnType(code) + "]  {\n" +
			code +
			"}\n"
		a

	}
}


/** BDD END **/