package info.gratour.db;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

public class UserEntry {

    public static final Type TYPE = new TypeToken<UserEntry>(){}.getType();

    private String userName;
    private String passwdSeed;
    private String passwdMd5Hex;
    private Long subAreaId;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPasswdSeed() {
        return passwdSeed;
    }

    public void setPasswdSeed(String passwdSeed) {
        this.passwdSeed = passwdSeed;
    }

    public String getPasswdMd5Hex() {
        return passwdMd5Hex;
    }

    public void setPasswdMd5Hex(String passwdMd5Hex) {
        this.passwdMd5Hex = passwdMd5Hex;
    }

    public Long getSubAreaId() {
        return subAreaId;
    }

    public void setSubAreaId(Long subAreaId) {
        this.subAreaId = subAreaId;
    }

    @Override
    public String toString() {
        return "UserEntry{" +
                "userName='" + userName + '\'' +
                ", passwdSeed='" + passwdSeed + '\'' +
                ", passwdMd5Hex='" + passwdMd5Hex + '\'' +
                ", subAreaId=" + subAreaId +
                '}';
    }
}
