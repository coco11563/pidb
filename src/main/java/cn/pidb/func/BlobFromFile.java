/**
 * a user defined function for pidb which supports generate a Blob object from a local file.
 * create by zhangzl419 on 2018-07-16 at cnic
 */
package cn.pidb.func;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.Blob$;
import org.neo4j.values.storable.Blob;
import java.io.File;

public class BlobFromFile
{
    @UserFunction("Blob.fromFile")
    @Description("generate a blob object from the given file")
    public Blob blobFromFile(@Name("filePath") String filePath)
    {
        Blob blob = null;

        if (filePath == null || filePath == "")
        {
            blob = null;
        }
        else {
            File localFile = new File(filePath);

            blob = Blob$.MODULE$.fromFile(localFile);
            System.out.println(localFile);
        }

        return blob;
    }
}
