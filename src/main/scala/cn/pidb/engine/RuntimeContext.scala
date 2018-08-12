package cn.pidb.engine

import scala.collection.mutable.{Map => MMap}

/**
  * Created by bluejoe on 2018/8/12.
  */
class RuntimeContext {
  private val _runtimeContext = MMap[String, Any]();

  def putRuntimeContext(key: String, value: Any) = _runtimeContext(key) = value;

  def putRuntimeContext[T](value: Any)(implicit manifest: Manifest[T]) = _runtimeContext(manifest.runtimeClass.getName) = value;

  def getRuntimeContext(key: String): Any = _runtimeContext(key);

  def getRuntimeContext[T]()(implicit manifest: Manifest[T]): T = _runtimeContext(manifest.runtimeClass.getName).asInstanceOf[T];
}