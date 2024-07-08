package com.thinking.machines.orm.tools;
public class Configuration implements java.io.Serializable
{
private String jdbc_driver;
private String connection_url;
private String username;
private String password;
private String database;
private String packageName;
private String jarFile;
public Configuration()
{
this.jdbc_driver="";
this.connection_url="";
this.username="";
this.password="";
this.database="";
this.packageName="";
this.jarFile="";
}
public void setPackageName(String packageName)
{
this.packageName=packageName;
}
public String getPackageName()
{
return this.packageName;
}
public void setJarFile(String jarFile)
{
this.jarFile=jarFile;
}
public String getJarFile()
{
return this.jarFile;
}
public void setJdbcDriver(java.lang.String jdbc_driver)
{
this.jdbc_driver=jdbc_driver;
}
public java.lang.String getJdbcDriver()
{
return this.jdbc_driver;
}
public void setConnectionUrl(java.lang.String connection_url)
{
this.connection_url=connection_url;
}
public java.lang.String getConnectionUrl()
{
return this.connection_url;
}
public void setUsername(java.lang.String username)
{
this.username=username;
}
public java.lang.String getUsername()
{
return this.username;
}
public void setPassword(java.lang.String password)
{
this.password=password;
}
public java.lang.String getPassword()
{
return this.password;
}
public void setDatabase(java.lang.String database)
{
this.database=database;
}
public java.lang.String getDatabase()
{
return this.database;
}
}