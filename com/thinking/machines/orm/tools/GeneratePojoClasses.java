package com.thinking.machines.orm.tools;
import com.google.gson.*;
import java.io.*;
import java.util.*;
import java.sql.*;
import javax.tools.*;

class DataType
{
public String type;
public String name;
}
public class GeneratePojoClasses
{
private static final Map<String, String> SqlToJavaTypes= new HashMap<>();

    static {
        SqlToJavaTypes.put("TINYINT", "java.lang.Boolean");
        SqlToJavaTypes.put("SMALLINT", "java.lang.Short");
        SqlToJavaTypes.put("MEDIUMINT", "java.lang.Integer");
        SqlToJavaTypes.put("INT", "java.lang.Integer");
        SqlToJavaTypes.put("INTEGER", "java.lang.Integer");
        SqlToJavaTypes.put("BIGINT", "java.lang.Long");
        SqlToJavaTypes.put("FLOAT", "java.lang.Long");
        SqlToJavaTypes.put("DOUBLE", "java.lang.Double");
        SqlToJavaTypes.put("REAL", "java.lang.Double");
        SqlToJavaTypes.put("DECIMAL", "java.math.BigDecimal");
        SqlToJavaTypes.put("NUMERIC", "java.math.BigDecimal");
        SqlToJavaTypes.put("BOOLEAN", "java.lang.Boolean");
        SqlToJavaTypes.put("BOOL", "java.lang.Boolean");
        SqlToJavaTypes.put("DATE", "java.sql.Date");
        SqlToJavaTypes.put("DATETIME", "java.sql.Timestamp");
        SqlToJavaTypes.put("TIME", "java.sql.Time");
        SqlToJavaTypes.put("CHAR", "java.lang.String");
        SqlToJavaTypes.put("VARCHAR", "java.lang.String");
        SqlToJavaTypes.put("BINARY", "byte[]");
        SqlToJavaTypes.put("VARBINARY", "byte[]");
        SqlToJavaTypes.put("TINYBLOB", "byte[]");
        SqlToJavaTypes.put("BLOB", "byte[]");
        SqlToJavaTypes.put("MEDIUMBLOB", "byte[]");
        SqlToJavaTypes.put("LONGBLOB", "byte[]");
        SqlToJavaTypes.put("TINYTEXT", "java.lang.String");
        SqlToJavaTypes.put("TEXT", "java.lang.String");
        SqlToJavaTypes.put("MEDIUMTEXT", "java.lang.String");
        SqlToJavaTypes.put("LONGTEXT", "java.lang.String");
    }


public static void main(String gg[])
{
try
{
File file=new File("conf.json");
if(!file.exists())
{
System.out.println("conf.json file not found");
return;
}
if(file.length()==0)
{
System.out.println("conf.json file is empty");
return;
}
Gson gson=new Gson();
FileReader fileReader=new FileReader(file);
Configuration configuration=gson.fromJson(fileReader,Configuration.class);
fileReader.close();
String packageName=configuration.getPackageName().replace(".",File.separator);

RandomAccessFile randomAccessFile;
String s;
Class clz=Class.forName(configuration.getJdbcDriver());
Connection connection=DriverManager.getConnection(configuration.getConnectionUrl(),configuration.getUsername(),configuration.getPassword());
Statement statement=connection.createStatement();
ResultSet resultSet=statement.executeQuery("show tables");
List<String> lists=new LinkedList<>();
List<DataType> dataTypeList=new LinkedList<>();
while(resultSet.next())
{
s=resultSet.getString(1);
lists.add(s);
System.out.println("Class Name. "+s);
}
resultSet.close();
PreparedStatement preparedStatement=null;
String sql;
String columnName;
String dataType;
String columnKey;
String columnDefault;
String extra;
String className;
String parentTableName;
String parentColumnName;
String tableName;
String tableType;
File srcDir=new File(packageName+File.separator+"src");
if(!srcDir.exists()) srcDir.mkdirs();
for(int i=0;i<lists.size();i++)
{
tableName=lists.get(i);
s=tableName;
s=columnConverter(s);
className=Character.toUpperCase(s.charAt(0))+s.substring(1);
file=new File(srcDir,className+".java");

if(file.exists()) file.delete();

randomAccessFile=new RandomAccessFile(file,"rw");
preparedStatement=connection.prepareStatement("SELECT TABLE_TYPE FROM  INFORMATION_SCHEMA.TABLES WHERE     TABLE_SCHEMA = ? AND TABLE_NAME = ?");
preparedStatement.setObject(1,configuration.getDatabase());
preparedStatement.setObject(2,tableName);
resultSet=preparedStatement.executeQuery();
resultSet.next();
tableType=resultSet.getString("TABLE_TYPE");
resultSet.close();
preparedStatement.close();
if(tableType.equalsIgnoreCase("VIEW")){
randomAccessFile.writeBytes("import com.thinking.machines.orm.annotations.*;\n");
randomAccessFile.writeBytes("@View(name=\""+tableName+"\")\n");
randomAccessFile.writeBytes("public class "+className+" implements java.io.Serializable\n{\n");
}
else{




randomAccessFile.writeBytes("import com.thinking.machines.orm.annotations.*;\n");
randomAccessFile.writeBytes("@Table(name=\""+tableName+"\")\n");
randomAccessFile.writeBytes("public class "+className+" implements java.io.Serializable\n{\n");
}

// extracting meta data of perticular table
sql="SELECT      C.COLUMN_NAME,     C.DATA_TYPE,    C.COLUMN_KEY,   C.COLUMN_DEFAULT,     C.EXTRA,     KCU.REFERENCED_TABLE_NAME AS PARENT_TABLE_NAME,     KCU.REFERENCED_COLUMN_NAME AS PARENT_COLUMN_NAME                               FROM      INFORMATION_SCHEMA.COLUMNS AS C LEFT JOIN      INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU      ON C.TABLE_SCHEMA = KCU.TABLE_SCHEMA      AND C.TABLE_NAME = KCU.TABLE_NAME      AND C.COLUMN_NAME = KCU.COLUMN_NAME      AND KCU.REFERENCED_TABLE_NAME IS NOT NULL WHERE      C.TABLE_SCHEMA = '"+configuration.getDatabase()+"'  AND C.TABLE_NAME = '"+tableName+"'; ";
//sql="SELECT      C.COLUMN_NAME,     C.DATA_TYPE,    C.COLUMN_KEY,   C.COLUMN_DEFAULT,     C.EXTRA,     KCU.REFERENCED_TABLE_NAME AS PARENT_TABLE_NAME,     KCU.REFERENCED_COLUMN_NAME AS PARENT_COLUMN_NAME,   T.TABLE_TYPE FROM      INFORMATION_SCHEMA.COLUMNS AS C LEFT JOIN      INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU      ON C.TABLE_SCHEMA = KCU.TABLE_SCHEMA      AND C.TABLE_NAME = KCU.TABLE_NAME      AND C.COLUMN_NAME = KCU.COLUMN_NAME      AND KCU.REFERENCED_TABLE_NAME IS NOT NULL JOIN      INFORMATION_SCHEMA.TABLES AS T      ON C.TABLE_SCHEMA = T.TABLE_SCHEMA      AND C.TABLE_NAME = T.TABLE_NAME WHERE      C.TABLE_SCHEMA = '"+configuration.getDatabase()+"'      AND C.TABLE_NAME ='"+tableName+"'; ";
resultSet=statement.executeQuery(sql);

while(resultSet.next())
{
columnName = resultSet.getString("COLUMN_NAME");
dataType = resultSet.getString("DATA_TYPE");
columnKey = resultSet.getString("COLUMN_KEY");
columnDefault = resultSet.getString("COLUMN_DEFAULT");
extra = resultSet.getString("EXTRA");
parentTableName=resultSet.getString("PARENT_TABLE_NAME");
parentColumnName=resultSet.getString("PARENT_COLUMN_NAME");

if(extra.equals("auto_increment")) randomAccessFile.writeBytes("@AutoIncrement\n");
if(columnKey.equals("PRI")) randomAccessFile.writeBytes("@PrimaryKey\n");
if(columnKey.equals("MUL")) randomAccessFile.writeBytes("@ForeignKey(parent=\""+parentTableName+"\",column=\""+parentColumnName+"\")\n");
randomAccessFile.writeBytes("@Column(name=\""+columnName+"\")\n");

columnName=columnConverter(columnName);// this method is used for convert columnName to camelCase
dataType=SqlToJavaTypes.get(dataType.toUpperCase());// this map is used for convert sql data type to java data type

randomAccessFile.writeBytes("private "+dataType+" "+columnName+";\n");
DataType dt=new DataType();
dt.type=dataType;
dt.name=columnName;
dataTypeList.add(dt);
}
DataType dt;
String setterName;
String getterName;
randomAccessFile.writeBytes("\n");
for(int j=0;j<dataTypeList.size();j++)
{
dt=dataTypeList.get(j);
setterName="set"+Character.toUpperCase(dt.name.charAt(0))+dt.name.substring(1);
getterName="get"+Character.toUpperCase(dt.name.charAt(0))+dt.name.substring(1);
randomAccessFile.writeBytes("public void "+setterName+"("+dt.type+" "+dt.name+")\n");
randomAccessFile.writeBytes("{\n");
randomAccessFile.writeBytes("this."+dt.name+"="+dt.name+";\n");
randomAccessFile.writeBytes("}\n");
randomAccessFile.writeBytes("public "+dt.type+" "+getterName+"()\n");
randomAccessFile.writeBytes("{\n");
randomAccessFile.writeBytes("return this."+dt.name+";\n");
randomAccessFile.writeBytes("}\n");
}

dataTypeList.clear();
randomAccessFile.writeBytes("}\n");
randomAccessFile.close();
resultSet.close();

}// for loop list ends here

File outputDir = new File(packageName+ File.separator + "classes");
if (!outputDir.exists()) {
outputDir.mkdirs();
}

File[] javaFiles = srcDir.listFiles((dir, name) -> name.endsWith(".java"));
if (javaFiles == null || javaFiles.length == 0) {
System.out.println("No Java files found to compile.");
return;
 }
        
        // Compile all listed Java files
        List<String> compileOptions = new ArrayList<>();
        compileOptions.add("-d");
        compileOptions.add(outputDir.getPath());
        for (File javaFile : javaFiles) {
            compileOptions.add(javaFile.getPath());
        }

JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
int result = compiler.run(null, null, null, compileOptions.toArray(new String[0]));
if (result != 0) {
System.out.println("Compilation failed.");
return;
} else {
System.out.println("Compilation succeeded.");
}
        // Step 3: Create a JAR file in the dist directory
        File distDir = new File(packageName+ File.separator + "dist");
        if (!distDir.exists()) {
            distDir.mkdirs();
        }
        File jarFile = new File(distDir,configuration.getJarFile()+".jar");

ProcessBuilder processBuilder = new ProcessBuilder("jar", "cf", jarFile.getPath(), "-C", outputDir.getPath(),".");
processBuilder.inheritIO();
Process process = processBuilder.start();
result = process.waitFor();
if (result == 0) {
System.out.println("JAR file created successfully. "+configuration.getJarFile()+".jar");
} else {
System.out.println("Failed to create JAR file.");
}

statement.close();
connection.close();
}catch(Exception exception)
{
System.out.println(exception.getMessage());
}
}
private static String columnConverter(String g)
{
int i=g.indexOf("_");
if(i==-1) return g;
String str1;
char str2;
String str3;
for(int j=0;j<g.length();)
{
i=g.indexOf("_");
if(i==-1) break;
str1=g.substring(0,i);
str2=Character.toUpperCase(g.charAt(i+1));
str3=g.substring(i+2);
g=str1+str2+str3;
j=i;
}
return g;
}
}

