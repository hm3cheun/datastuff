import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.*;
import org.apache.hadoop.io.IOUtils;
 
public class HiveJdbcClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
  private Connection con;
  private Statement stmt;
  /**
   * @param args
   * @throws SQLException
   */

  public static void WriteToHdfs(String localSrc, String dst)  throws Exception
  { 
	//Input stream for the file in local file system to be written to HDFS
	InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
	 
	//Get configuration of Hadoop system
	Configuration conf = new Configuration();
	System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));
	 
	//Destination file in HDFS
	FileSystem fs = FileSystem.get(URI.create(dst), conf);
	OutputStream out = fs.create(new Path(dst));
	 
	//Copy file from local to HDFS
	IOUtils.copyBytes(in, out, 4096, true);
	 
	System.out.println(dst + " copied to HDFS");
  }
  public static void CopyAll() throws Exception
  { 
		System.out.println("Copying Files to Hdfs");
		WriteToHdfs("/root/Reddit/top25", "/tmp/data/Reddit/top25");
		WriteToHdfs("/root/Reddit/top25_1", "/tmp/data/Reddit/top25_1");
		WriteToHdfs("/root/Reddit/top25_2", "/tmp/data/Reddit/top25_2");
		WriteToHdfs("/root/Reddit/top25_3", "/tmp/data/Reddit/top25_3");
		WriteToHdfs("/root/4square/4square_checkin", "/tmp/data/4square/4square_checkin");
		WriteToHdfs("/root/4square/4sq_cities", "/tmp/data/4square/4sq_cities");
		WriteToHdfs("/root/4square/4sq_poi", "/tmp/data/4square/4sq_poi");
		WriteToHdfs("/root/4square/4square_checkin_1", "/tmp/data/4square/4square_checkin_1");
		WriteToHdfs("/root/4square/4square_checkin_2", "/tmp/data/4square/4square_checkin_2");
		WriteToHdfs("/root/4square/4square_checkin_3", "/tmp/data/4square/4square_checkin_3");
		WriteToHdfs("/root/Wikipedia/wiki_clickstream_temp1", "/tmp/data/Wikipedia/wiki_clickstream_temp1");
		System.out.println("Finished Copying Files to Hdfs");
   }

  public HiveJdbcClient()
  {
	try {
      	Class.forName(driverName);
	con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
	stmt = con.createStatement();
    	} catch (ClassNotFoundException e) {
      		e.printStackTrace();
      		System.exit(1);
    	}
	catch (SQLException e) {
      		e.printStackTrace();
      		System.exit(1);
    	}
  }

  public void test(String dbname)
  {	
	try {
		
		stmt.execute("DROP TABLE IF EXISTS "+ dbname+".4sq_poi");
		stmt.execute("DROP TABLE IF EXISTS "+ dbname+".4sq_cities");
		stmt.execute("DROP TABLE IF EXISTS "+ dbname+".4sq_checkpoint");
		stmt.execute("DROP TABLE IF EXISTS "+ dbname+".reddit");
		stmt.execute("DROP TABLE IF EXISTS "+ dbname+".wiki");
		//stmt.execute("DROP DATABASE IF EXISTS " +dbname);
		//stmt.execute("CREATE DATABASE IF NOT EXISTS "+dbname);
		stmt.execute("USE "+dbname);
		
	}
	catch (SQLException e) {
      		e.printStackTrace();
      		System.exit(1);
    	}
  }
  public void createTable()
  {	
	try {
		System.out.println("Creating Table 4sq_poi");
		String create_4sq_poi = "CREATE TABLE IF NOT EXISTS 4sq_poi(v_id string, lat float, long float, cat_name string, country_code string) ROW FORMAT DELIMITED 						 FIELDS TERMINATED BY \'\t\' ESCAPED BY \'\"\' LINES TERMINATED BY \'\n\'STORED as TEXTFILE";
		stmt.execute(create_4sq_poi);
		System.out.println("Creating Table 4sq_cities");
		String create_4sq_cities = "CREATE TABLE IF NOT EXISTS 4sq_cities(city_name string, city_lat float, city_long float, country_code string, country_name string) 					ROW FORMAT DELIMITED FIELDS TERMINATED BY  \'\t\' ESCAPED BY \'\"\' LINES TERMINATED BY \'\n\' STORED as TEXTFILE";
		stmt.execute(create_4sq_cities);
		System.out.println("Creating Table 4sq_checkpoint");
		String create_4sq_checkpoint ="CREATE TABLE IF NOT EXISTS 4sq_checkpoint(u_id string, v_id string, date_time string, offset string) ROW FORMAT DELIMITED 							FIELDS TERMINATED BY  \'\t\' ESCAPED BY \'\"\' LINES TERMINATED BY \'\n\' STORED as TEXTFILE";
		stmt.execute(create_4sq_checkpoint);
		System.out.println("Creating Table reddit");
		String create_reddit ="CREATE TABLE IF NOT EXISTS reddit(ranked SMALLINT,subr string, created_utc string, score int,domain string,id string,  						title string,author string,ups SMALLINT, downs SMALLINT, num_comments SMALLINT, permalink string,link_flair_text string,over_18 string, 					thumbnail string, subreddit_id string, editied string, link_flair_css_class string, author_flair_css_class string, is_self string, name 					string, url  string,  distinguished string) ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\' STORED AS TEXTFILE";
		stmt.execute(create_reddit);
		System.out.println("Creating Table wiki");
		String create_wiki ="CREATE TABLE IF NOT EXISTS Wiki(prev string,curr string,type string,    n int) ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\' with serdeproperties (\"separatorChar\" = \"\t\",\"quoteChar\"= \"\'\") STORED AS TEXTFILE";
		stmt.execute(create_wiki);

		
	}
	catch (SQLException e) {
      		e.printStackTrace();
      		System.exit(1);
    	}
  }
 public void loadData()
 {
	try {
	System.out.println("Loading Data into 4sq_poi");
	String query="LOAD DATA INPATH \'/tmp/data/Wikipedia/wiki_clickstream_temp1\' INTO TABLE wiki"; 
        stmt.execute(query);
	System.out.println("Loading Data into reddit");
	query="LOAD DATA INPATH '/tmp/data/Reddit/top25_2' INTO TABLE reddit";
	stmt.execute(query);
	System.out.println("Loading Data into 4sq_checkpoint");
	query="LOAD DATA INPATH '/tmp/data/4square/4square_checkin_3' INTO TABLE 4sq_checkpoint";
	stmt.execute(query);
	System.out.println("Loading Data into 4sq_poi");
	query="LOAD DATA INPATH '/tmp/data/4square/4sq_poi' INTO TABLE 4sq_poi";
	stmt.execute(query);
	System.out.println("Loading Data into 4sq_cities");
	query="LOAD DATA INPATH '/tmp/data/4square/4sq_cities' INTO TABLE 4sq_cities";
	stmt.execute(query);
	}
	catch (SQLException e) {
      		e.printStackTrace();
      		System.exit(1);
    	}
 }

 public void TransformTable()
	{
	try {

		System.out.println("Transforming : 4sq_poi_us");
		String query="create table if not exists 4sq_poi_us as select * from 4sq_poi where country_code ==\"US\""; 
		stmt.execute(query);
		System.out.println("Transforming :  4sq_cities_us");
		query="create table if not exists 4sq_cities_us as select * from 4sq_cities where country_code ==\"US\"";
		stmt.execute(query);
		System.out.println("Transforming :  poi_us_join_checkpoint");
		query="create table if not exists poi_us_join_checkpoint as select  t1.v_id as v_id,  t2.cat_name as category, t2.lat as lat, t2.long as long, t2.country_code as country_code, t1.u_id as u_id, t1.date_time as date_time from 4sq_checkpoint t1, 4sq_poi_us t2 where t1.v_id == t2.v_id";
		stmt.execute(query);
		System.out.println("Transforming :  4sq_us");
		query="create table if not exists 4sq_us as select  t1.v_id as v_vid, t1.country_code as country_code, t1.category as category, t1.lat as lat, t1.long as long, t1.u_id as u_id, t1.date_time as date_time, t2.city_name as city_name, t2.city_long as city_long, t2.city_lat as city_lat, t2.country_name as country_name from  poi_us_join_checkpoint t1, 4sq_cities_us t2  where t1.country_code == t2.country_code";
		stmt.execute(query);
		System.out.println("Transforming :  4sq_us_day");
		query="create table if not exists 4sq_us_day as select  v_vid, country_code, substring(date_time,9,2) as dayofmonth, category, lat, long, u_id, date_time, city_name, city_long, city_lat, country_name from 4sq_us";
		stmt.execute(query);
		System.out.println("Transforming :  reddit_april_day");
		query="create table if not exists reddit_april_day as select     ranked,     subr,    created_utc,    substring(created_utc,9,2) as dayofmonth,    substring(created_utc,6,2) as month, score, domain, id,    title,    author,    ups,    downs,    num_comments,    permalink,    link_flair_text, over_18,    thumbnail,    subreddit_id, editied,    link_flair_css_class,    author_flair_css_class,    is_self,    name,    url,    distinguished from reddit";
		stmt.execute(query);
		System.out.println("Transforming : temp_4sq_us_day");
		query="create table if not exists temp_4sq_us_day as SELECT * FROM 4sq_us_day t1 limit 100000";  
		stmt.execute(query);
		System.out.println("Transforming : temp_reddit_april_day");
		query="create table if not exists temp_reddit_april_day as select * from reddit_april_day limit 10000"; 
		stmt.execute(query); 
		System.out.println("Transforming : wiki_n_not_null");
		query="create table if not exists wiki_n_not_null as SELECT * FROM wiki where n is not null";
		stmt.execute(query);

		System.out.println("Transforming : reddit_joined_4sq");
		query="create table if not exists reddit_joined_4sq as select t2.ranked, t2.subr, t2.created_utc, t2.dayofmonth, t2.month, t2.score, t2.domain, t2.id, t2.title, t2.author, t2.ups, t2.downs, t2.num_comments, t2.over_18, t2.subreddit_id, t2.is_self, t2.name, t2.url, t1.country_code, t1.city_name from temp_4sq_us_day t1, temp_reddit_april_day t2 where t1.dayofmonth == t2.dayofmonth";
		stmt.execute(query);
		}
		catch (SQLException e) {
	      		e.printStackTrace();
	      		System.exit(1);
	    	}
	}



  public static void main(String[] args) throws Exception  {
      
   CopyAll();
   HiveJdbcClient hive_client = new HiveJdbcClient();
   hive_client.test("testing");
   hive_client.createTable();
   hive_client.loadData();
   hive_client.TransformTable();

  }   
}
