package cn.pidb.func

import cn.pidb.functor.Functor
import org.neo4j.procedure.{Description, Name, UserFunction}
import org.neo4j.values.storable.Blob

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2018/11/30.
  */
class AIFunc {
  @UserFunction("AI.predict")
  @Description("predict")
  def predict(@Name("blob") blob: Blob, @Name("processorName") processorName: String): java.util.Map[String, Any] = {
    JavaConversions.mapAsJavaMap {
      if (blob == null)
        Map();
      else {
        try {
          Functor.get(processorName).predict(blob);
        }
        catch {
          case _ =>
            Map();
        }
      }
    }
  }
}
