package info.gratour.db;

import info.gratour.db.sql.MapperBuilder;
import org.junit.Ignore;
import org.junit.Test;
import scala.collection.mutable.ArrayBuffer;


@Ignore
public class MapperBuilderTest {

    @Test
    public void parse() {
        String sql = "          SELECT u.user_id, u.tenant_id, t.tenant_name, u.username, u.g_admin, u.d_admin,\n          u.create_uid, u2.username AS create_uname, u.create_time,\n          u.update_uid, u2.username AS update_uname, u.update_time\n          FROM t_user u " +
                "         LEFT JOIN t_user u2 ON u2.user_id = u.create_uid\n         RIGHT JOIN t_user u3 ON u3.user_id = u.update_uid\n          INNER JOIN t_tenant t on u.tenant_id = t.tenant_id";

        ArrayBuffer<MapperBuilder.Col> r = MapperBuilder.parse(sql);
        System.out.println(r);
    }

    @Test
    public void buildTest() {

    }
}
