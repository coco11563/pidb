package cn.pidb.engine

import scala.collection.mutable

/**
  * Created by bluejoe on 2018/11/28.
  */
object ThreadVars {
  val _vars = mutable.Map[(Thread, String), Any]();

  def put(name: String, value: Any, thread: Thread = Thread.currentThread()) = {
    _vars(thread -> name) = value;
  }

  def get[T](name: String, thread: Thread = Thread.currentThread()) = {
    _vars(thread -> name).asInstanceOf[T];
  }
}
