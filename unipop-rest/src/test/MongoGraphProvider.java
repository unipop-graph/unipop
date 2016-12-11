package test;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import org.restheart.Bootstrapper;
import org.unipop.test.UnipopGraphProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Created by sbarzilay on 12/11/16.
 */
public class MongoGraphProvider extends UnipopGraphProvider {
    private Fongo fongo;
    public MongoGraphProvider() throws IOException {
        this.fongo = new Fongo("mongo server 1");
        DB db = fongo.getDB("db");
        DBCollection mycollection = db.getCollection("mycollection");
        mycollection.insert(new BasicDBObject("name", "john"));
        Files.copy(Paths.get(this.getClass().getResource("/security.yaml").getPath()), Paths.get("/tmp/security.yaml"), StandardCopyOption.REPLACE_EXISTING);
        Bootstrapper.startup(this.getClass().getResource("/restheart.yaml").getPath());
    }
}
