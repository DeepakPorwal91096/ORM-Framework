package com.thinking.machines.orm;
import com.thinking.machines.orm.exceptions.*;
import com.thinking.machines.orm.tools.*;
import com.thinking.machines.orm.annotations.*;
import java.lang.annotation.*;
import java.lang.reflect.*;
import java.io.*;
import java.sql.*;
import com.google.gson.*;
import java.util.*;
import java.net.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.stream.*;
public class DataManager
{
private static DataManager dataManager;
private Class c;
private Connection connection;
private  static Configuration  configuration;
private static Map<String,Map<String,Object>> entityMap=null;

private DataManager()
{
dataManager=null;
}
public static DataManager getDataManager() throws DataException
{
if(dataManager!=null) return dataManager;
try
{
File file=new File("conf.json");
if(!file.exists())
{
throw new DataException("conf.json file not found");
}
if(file.length()==0)
{
throw new DataException("conf.json file not found");
}
FileReader fileReader=new FileReader(file);
Gson gson=new Gson();
configuration=gson.fromJson(fileReader,Configuration.class);
}catch(Exception exception)
{
throw new DataException(exception.getMessage());
}
dataManager=new DataManager();
return dataManager;
}
static public  void init() throws DataException // Necessary to call 
{
if(entityMap!=null) throw new DataException("Error: This method has already been called once and cannot be called again. " );

entityMap=new HashMap<>();

 String currentPath = System.getProperty("user.dir");// // Get the current working directory IMPORTANT


        currentPath=currentPath+File.separator+configuration.getPackageName().replace(".",File.separator)+File.separator+"classes";


java.nio.file.Path folderPath =java.nio.file.Paths.get(currentPath);
List<String> list=new LinkedList<>();
try (Stream<java.nio.file.Path> paths = Files.walk(folderPath)) {
paths.filter(path -> path.toString().endsWith(".class")).forEach(path->{list.add(path.toString());});
}catch(Exception exception)
{
throw new DataException("Unable to load Data Structure");
}
Class clz=null; 
try
{
int lastDotIndex=0;
String fileNameWithoutExtension;
int ii;
String newString="";
String className="";


boolean autoIncrement=false;
boolean primaryKey=false;
Table tableAnnotation=null;
Field[] fields=null;
Field f;
StringBuffer insertStr1=null;
StringBuffer insertStr2=null;
List<Method> insertLists=null;

StringBuffer updateStr1=null;
StringBuffer updateStr2=null;
List<Method> updateLists=null;
boolean updateComa=false;
boolean insertComa=false;
int numberOfMethods=0;
Map<String,Object> dataMap=null;

for(int k=0;k<list.size();k++)
{
lastDotIndex=list.get(k).lastIndexOf('.');
fileNameWithoutExtension = list.get(k).substring(0, lastDotIndex);
ii=fileNameWithoutExtension.indexOf("classes");
newString=fileNameWithoutExtension.substring(ii+8);
try
{
clz=Class.forName(newString);
className=clz.getName();
}catch(ClassNotFoundException cnfe)
{
System.out.println("ClassNotFound : "+cnfe);
return;
}
if(clz.isAnnotationPresent(View.class)) continue;
if(clz.isAnnotationPresent(Table.class)==false)
{
throw new DataException("Unable to process "+className+" (Table) annotation is not present.. Please specify @Table(name=\"table_name\")");
}

autoIncrement=false;
primaryKey=false;
entityMap.put(className.toUpperCase(),new HashMap<String,Object>());
tableAnnotation=(Table)clz.getAnnotation(Table.class);
fields=clz.getDeclaredFields();
insertStr1=new StringBuffer();
insertStr2=new StringBuffer();
insertStr1.append("insert into "+tableAnnotation.name()+" (");
insertStr2.append("values (");
insertLists=new LinkedList<>();

updateStr1=new StringBuffer();
updateStr2=new StringBuffer();
updateStr1.append("update "+tableAnnotation.name()+" set ");
updateStr2.append("");
updateLists=new LinkedList<>();
updateComa=false;
insertComa=false;
numberOfMethods=fields.length;
dataMap=entityMap.get(className.toUpperCase());


for(int i=0;i<fields.length;i++)
{
f=fields[i];
String fieldName = fields[i].getName();
if(f.isAnnotationPresent(AutoIncrement.class)){// here we are using reflection api for scanning all field that perticular annotation is present or not 
autoIncrement=true;
Method method=clz.getDeclaredMethod("get"+Character.toUpperCase(fieldName.charAt(0))+fieldName.substring(1));
dataMap.put("autoIncrementMethod",method);
}

if(f.isAnnotationPresent(PrimaryKey.class)){
PrimaryKey primaryAnnotation=f.getAnnotation(PrimaryKey.class);
Column columnAnnotation=f.getAnnotation(Column.class);
String columnName=columnAnnotation.name();
Method method=clz.getDeclaredMethod("get"+Character.toUpperCase(fieldName.charAt(0))+fieldName.substring(1));
String str="select * from "+tableAnnotation.name()+" where "+columnAnnotation.name()+"=?";
dataMap.put("primaryKeyQuery",str);
dataMap.put("primaryKeyMethod",method);
updateStr2.append("where "+columnAnnotation.name()+"=?");
primaryKey=true;
}

if(f.isAnnotationPresent(ForeignKey.class))
{
// here we are checking in parent table foreignKey is present or not if it is not present then we will raise exception
ForeignKey foreignKeyAnnotation=f.getAnnotation(ForeignKey.class);
String parent=foreignKeyAnnotation.parent();
String column=foreignKeyAnnotation.column();
Method method=clz.getDeclaredMethod("get"+Character.toUpperCase(fieldName.charAt(0))+fieldName.substring(1));
String str="select * from "+parent+" where "+column+"=?";
dataMap.put("foreignKeyQuery",str);
dataMap.put("foreignKeyMethod",method);
}

if(f.isAnnotationPresent(Column.class))
{
Column columnAnnotation=f.getAnnotation(Column.class);
String columnName=columnAnnotation.name();
Method method=clz.getDeclaredMethod("get"+Character.toUpperCase(fieldName.charAt(0))+fieldName.substring(1));
if(!primaryKey)
{
if(updateComa==true) updateStr1.append(",");
updateStr1.append(columnName+"=?");
updateLists.add(method);
updateComa=true;
}else primaryKey=false;


if(!autoIncrement){
if(insertComa==true) insertStr1.append(",");
insertStr1.append(columnName);
insertLists.add(method);
if(insertComa==true) insertStr2.append(",");
insertStr2.append("?");
insertComa=true;
} else autoIncrement=false;
}

}// fields loop ends here

insertStr1.append(")");
insertStr2.append(");");
insertStr1.append(" "+insertStr2);

updateStr1.append(" "+updateStr2);

dataMap.put("insertQuery",insertStr1.toString());
dataMap.put("insertMethods",insertLists);
dataMap.put("updateQuery",updateStr1.toString());
dataMap.put("updateMethods",updateLists);
}// list loop ends here

} catch (Exception e) {
throw new DataException(e.getMessage());
}// Scanning part ends here

}//init methods ends here



public void begin() throws DataException
{
try
{
c=Class.forName(configuration.getJdbcDriver());
connection=DriverManager.getConnection(configuration.getConnectionUrl(),configuration.getUsername(),configuration.getPassword());
}catch(Exception exception)
{
throw new DataException(exception.getMessage());
}
}
public int save(Object object) throws DataException
{
try
{
Class clz=object.getClass();
String className=clz.getName();
if(this.entityMap==null)
{
throw new DataException("Initialization error: Please ensure the init method is called.");
}
Map<String,Object> dataMap=entityMap.get(className.toUpperCase());
if(dataMap==null)
{
throw new DataException("Unable to process "+className+" (Table) annotation is not present.. Please specify @Table(name=\"table_name\")");
}

Method method=(Method)dataMap.get("autoIncrementMethod");
boolean autoIncrement=false;
PreparedStatement preparedStatement;
ResultSet resultSet;
Object obj;
if(method!=null)
{
autoIncrement=true;
Integer intvalue=(Integer)method.invoke(object);
if (intvalue!= null) {
throw new DataException(method.getName().replace("get","")+ " must be zero as it is set to auto-increment. Please leave it unset.");
}
}


method=(Method)dataMap.get("primaryKeyMethod");
if(method!=null)
{
Method m=(Method)dataMap.get("autoIncrementMethod");
if(m==null || m.getName().equals(method.getName())==false)
{
obj=method.invoke(object);
if(obj==null) throw new DataException("The property ' " +method.getName().replace("get","")+ " ' cannot be null.");
preparedStatement=connection.prepareStatement((String)dataMap.get("primaryKeyQuery"));
preparedStatement.setObject(1,obj);
resultSet=preparedStatement.executeQuery();
if(resultSet.next())
{
resultSet.close();
preparedStatement.close();
throw new DataException("Duplicate entry '"+method.getName().replace("get","")+"' for key '"+className+".PRIMARY'");
}
resultSet.close();
preparedStatement.close();
}
}

method=(Method)dataMap.get("foreignKeyMethod");
if(method!=null)
{
obj=method.invoke(object);
if(obj==null) throw new DataException("The property ' "+method.getName().replace("get","")+" ' cannot be null.");
preparedStatement=connection.prepareStatement((String)dataMap.get("foreignKeyQuery"));
preparedStatement.setObject(1,obj);
resultSet=preparedStatement.executeQuery();
if(resultSet.next()==false){
resultSet.close();
preparedStatement.close(); 
throw new DataException(method.getName().replace("get","")+"  Foreign key constraint violation. Ensure that the referenced data exists in the parent table.");
}
resultSet.close();
preparedStatement.close(); 
}


List<Method> lists=(List<Method>) dataMap.get("insertMethods");
if(lists==null) throw new DataException("Problem");

if(autoIncrement)
{
preparedStatement=connection.prepareStatement((String)dataMap.get("insertQuery"),Statement.RETURN_GENERATED_KEYS);
for(int i=0;i<lists.size();i++)
{
obj=lists.get(i).invoke(object);
if(obj==null) throw new DataException("The property ' "+lists.get(i).getName().replace("get","")+" ' cannot be null.");
preparedStatement.setObject(i+1,obj);
}
preparedStatement.executeUpdate();
resultSet=preparedStatement.getGeneratedKeys();
resultSet.next();
int code=resultSet.getInt(1);
resultSet.close();
preparedStatement.close();
System.out.println("Added...");
return code;
}
else{
preparedStatement=connection.prepareStatement((String)dataMap.get("insertQuery"));
for(int i=0;i<lists.size();i++)
{
obj=lists.get(i).invoke(object);
if(obj==null) throw new DataException("The property ' "+lists.get(i).getName().replace("get","")+" ' cannot be null.");
preparedStatement.setObject(i+1,obj);
}
preparedStatement.executeUpdate();
preparedStatement.close();
System.out.println("Added...");
return 0;
}
}catch(Exception exception)
{
throw new DataException(exception.getMessage());
}
}// save method ends here




public void update(Object object) throws DataException
{
Class clz=object.getClass();
String className=clz.getName();
boolean autoIncrement=false;
if(this.entityMap==null)
{
throw new DataException("Initialization error: Please ensure the init method is called.");
}
Map<String,Object> dataMap=entityMap.get(className.toUpperCase());
if(dataMap==null)
{
throw new DataException("Unable to process "+className+" (Table) annotation is not present.. Please specify @Table(name=\"table_name\")");
}
try
{

PreparedStatement preparedStatement=null;
ResultSet resultSet=null;
Method method;
method=(Method)dataMap.get("primaryKeyMethod");
Object obj;
if(method!=null)
{
obj=method.invoke(object);
if(obj==null) throw new DataException("The property ' " +method.getName().replace("get","")+ " ' cannot be null.");
preparedStatement=connection.prepareStatement((String)dataMap.get("primaryKeyQuery"));
preparedStatement.setObject(1,obj);
resultSet=preparedStatement.executeQuery();
if(resultSet.next()==false)
{
resultSet.close();
preparedStatement.close();
throw new DataException("Primary key '"+method.getName().replace("get","")+"' with value "+obj+ " not found");
}
resultSet.close();
preparedStatement.close();
}

method=(Method)dataMap.get("foreignKeyMethod");
if(method!=null)
{
obj=method.invoke(object);
if(obj==null) throw new DataException("The property ' "+method.getName().replace("get","")+" ' cannot be null.");
preparedStatement=connection.prepareStatement((String)dataMap.get("foreignKeyQuery"));
preparedStatement.setObject(1,obj);
resultSet=preparedStatement.executeQuery();
if(resultSet.next()==false){
resultSet.close();
preparedStatement.close(); 
throw new DataException(method.getName().replace("get","")+"  Foreign key constraint violation. Ensure that the referenced data exists in the parent table.");
}
resultSet.close();
preparedStatement.close(); 
}


List<Method> lists=(List<Method>) dataMap.get("updateMethods");

preparedStatement=connection.prepareStatement((String)dataMap.get("updateQuery"));
int i=0;
for(i=0;i<lists.size();i++)
{
obj=lists.get(i).invoke(object);
if(obj==null) throw new DataException("The property ' "+lists.get(i).getName().replace("get","")+" ' cannot be null.");
preparedStatement.setObject(i+1,obj);
}
method=(Method)dataMap.get("primaryKeyMethod");
preparedStatement.setObject(i+1,method.invoke(object));
int row=preparedStatement.executeUpdate();
preparedStatement.close();
if(row>0) System.out.println("Updated...");
else System.out.println("Not updated...");
}catch(Exception exception)
{
throw new DataException(exception.getMessage());
}
}// update methods ends here 

public int delete(Class clz,Object value) throws DataException
{
// 111   SELECT      TABLE_NAME,      COLUMN_NAME FROM      INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE  REFERENCED_TABLE_NAME = 'course';

String className=clz.getName();
boolean autoIncrement=false;
if(clz.isAnnotationPresent(Table.class)==false)
{
throw new DataException("Unable to process "+className+" (Table) annotation is not present.. Please specify @Table(name=\"table_name\")");
}
Table tableAnnotation=(Table)clz.getAnnotation(Table.class);
try
{
Statement statement=connection.createStatement();
ResultSet resultSet=statement.executeQuery("select TABLE_NAME,COLUMN_NAME from information_schema.Key_column_usage where referenced_table_name='"+tableAnnotation.name()+"'");
String tableName;
String columnName;
boolean exists=false;
Statement stats;
ResultSet rs;
while(resultSet.next())
{
tableName=resultSet.getString("TABLE_NAME");
columnName=resultSet.getString("COLUMN_NAME");
stats=connection.createStatement();
String sql="select * from "+tableName+" where "+columnName+"="+value;
rs=stats.executeQuery(sql);
exists=rs.next();
rs.close();
stats.close();
if(exists){
statement.close();
resultSet.close();
throw new DataException("Unable to delete  the selected item because it is linked to other records in the table " + tableName + ".");
}
}
statement.close();
resultSet.close();

Field[] fields=clz.getDeclaredFields();
Field f;
for(int i=0;i<fields.length;i++)
{
f=fields[i];
if(f.isAnnotationPresent(PrimaryKey.class))
{
// here we can check that primaryKey is  exists or not if it's not exists then raise exception
PrimaryKey primaryAnnotation=f.getAnnotation(PrimaryKey.class);
Column columnAnnotation=f.getAnnotation(Column.class);
columnName=columnAnnotation.name();
String sql="delete from "+tableAnnotation.name()+" where "+columnAnnotation.name()+"=?";
statement=connection.createStatement();
PreparedStatement preparedStatement=connection.prepareStatement(sql);
preparedStatement.setObject(1,value);
int rowEffected=preparedStatement.executeUpdate();
preparedStatement.close();
if(rowEffected==0) throw new DataException("Unable to delete from table " + tableAnnotation.name() +" where " + columnAnnotation.name() + " = " + value +" because it does not exist.");
}
}
}catch(Exception exception)
{
throw new DataException(exception.getMessage());
}
return 1;
}// delete methods ends here 






private List<Object> lists;
String sqlQuery;
Class claz;
public DataManager query(Class clz) throws DataException {
try
{
claz=clz;
lists=new LinkedList<>();
String className=clz.getName();
if(clz.isAnnotationPresent(Table.class)==false && clz.isAnnotationPresent(View.class)==false)
{
throw new DataException("Unable to process "+className+" (Table) annotation is not present.. Please specify @Table(name=\"table_name\")");
}
if(clz.isAnnotationPresent(Table.class))
{
Table tableAnnotation=(Table)clz.getAnnotation(Table.class);
sqlQuery="select * from "+tableAnnotation.name();
}else if(clz.isAnnotationPresent(View.class))
{
View viewAnnotation=(View)clz.getAnnotation(View.class);
sqlQuery="select * from "+viewAnnotation.name();
}
return this;
}catch(Exception exception)
{
throw new DataException(exception.getMessage());
}
}//query method ends here

public DataManager where(Object args)
{
sqlQuery+=" where "+args;
return this;
}
public DataManager eq(Object args) // equal to
{
sqlQuery+="="+args;
return this;
}

public DataManager or(Object args)
{
sqlQuery+=" OR "+args;
return this;
}
public DataManager and(Object args)
{
sqlQuery+=" AND "+args;
return this;
}

public DataManager lt(Object args) // less than 
{
sqlQuery+="<"+args;
return this;
}
public DataManager gt(Object args) // greater than
{
sqlQuery+=">"+args;
return this;
}

public DataManager ge(Object args) // greater than equal to
{
sqlQuery+=">="+args;
return this;
}
public DataManager le(Object args) // less than equal to
{
sqlQuery+="<="+args;
return this;
}
public DataManager ne(Object args) // not equal to 
{
sqlQuery+="!="+args;
return this;
}
public DataManager orderBy(Object args) // orderBy
{
sqlQuery+=" Order by "+args;
return this;
}
public DataManager between(Object args1,Object args2)
{
sqlQuery+=" between '"+args1+"' and '"+args2+"'";
return this;
}
public DataManager ascending()
{
sqlQuery+=" ASC";
return this;
}
public DataManager descending()
{
sqlQuery+=" DESC";
return this;
}

public Object fire() throws DataException
{
try
{
Statement statement=connection.createStatement();
ResultSet resultSet=statement.executeQuery(sqlQuery);
Object obj;
Field[] fields;
Field f;
Method methods[];
while(resultSet.next())
{
obj = claz.newInstance();
fields=claz.getDeclaredFields();
methods=claz.getDeclaredMethods();
for(int i=0;i<fields.length;i++)
{
f=fields[i];
Class c=f.getType();
Column columnAnnotation=f.getAnnotation(Column.class);
String columnName=columnAnnotation.name();
Object rso = resultSet.getObject(columnName);
Class targetClass =Class.forName(c.getName());
String methodName="set"+Character.toUpperCase(f.getName().charAt(0))+f.getName().substring(1);
Class parameter[]=new Class[1];
parameter[0]=Class.forName(c.getName());
Method method=claz.getDeclaredMethod(methodName,parameter);
method.invoke(obj,targetClass.cast(rso));
}
lists.add(obj);
}
statement.close();
resultSet.close();
return lists;
}catch(Exception exception)
{
throw new DataException(exception.getMessage());
}
}

public void end() throws DataException
{
try
{
connection.close();
}catch(Exception exception)
{
throw new DataException(exception.getMessage());
}
}
}
