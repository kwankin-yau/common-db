package info.gratour.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class DbConfig {
    private String jdbcUrl;
    private String username;
    private String password;
    private int maximumPoolSize = 10;

    public DbConfig() {
    }

    public String getJdbcUrl() {
        return this.jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    public HikariConfig toHikariConfig() {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(this.jdbcUrl);
        if (this.username != null)
            config.setUsername(this.username);
        if (this.password != null)
            config.setPassword(this.password);
        config.setLeakDetectionThreshold(100_000);
        config.setMaximumPoolSize(maximumPoolSize);

        return config;
    }

    public DataSource toDataSource() {
        HikariConfig config = toHikariConfig();
        return new HikariDataSource(config);
    }

    public DataSource toDataSource(HikariConfig config) {
        return new HikariDataSource(config);
    }

    @Override
    public String toString() {
        return "DbConfig{" +
                "jdbcUrl='" + jdbcUrl + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
