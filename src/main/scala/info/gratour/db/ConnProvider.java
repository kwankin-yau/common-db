package info.gratour.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public interface ConnProvider {

    DataSource dataSource();

    default Connection getConn() {
        try {
            return dataSource().getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
