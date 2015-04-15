package com.ceren.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class Application implements Serializable{
	List<String> links;
	List<WeatherData> weathers;

	public static void main(String[] args) {
		Application app=new Application();
//		app.getAllLinks();
//		app.visitLinks();
//		try {
//			app.writeToFile();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		app.startSpark();
	}
	
	
	
	public void getAllLinks(){
		links=new ArrayList<String>();
		String content = null;
		URLConnection connection = null;
		try {
		  connection =  new URL("https://convertale.com/challenge/intern/weather/all.html").openConnection();
		  Scanner scanner = new Scanner(connection.getInputStream());
		  scanner.useDelimiter("\\Z");
		  
		  while (scanner.hasNext()){
			  content = scanner.nextLine();
			  if (content.indexOf("href")>-1){
				  content=content.substring(content.indexOf("\"")+5, content.lastIndexOf("\""));
				  
				  links.add("https"+content);
			  }
		  }
		}catch ( Exception ex ) {
		    ex.printStackTrace();
		}
	}
	
	public void visitLinks(){
		weathers=new ArrayList<WeatherData>();
		for (String url:links){
			//url
			String content = null;
			URLConnection connection = null;
			try {
			  connection =  new URL(url).openConnection();
			  Scanner scanner = new Scanner(connection.getInputStream());
			  scanner.useDelimiter("\\Z");
			  
			  WeatherData weatherData=new WeatherData();
			  
			  while (scanner.hasNext()){
				  content = scanner.nextLine();

				  if (content.indexOf("<b>mean</b>")>-1) {
					  content=content.substring(content.indexOf("<span>")+6, content.lastIndexOf("</span>")-1);
					  weatherData.setMean(Integer.parseInt(content));
				  }
				  //min
				  if (content.indexOf("<b>min</b>")>-1) {
					  content=content.substring(content.indexOf("<span>")+6, content.lastIndexOf("</span>")-1);
					  weatherData.setMin(Integer.parseInt(content));
				  }
				  //max
				  if (content.indexOf("<b>max</b")>-1){
					  content=content.substring(content.indexOf("<span>")+6, content.lastIndexOf("</span>")-1);
					  weatherData.setMax(Integer.parseInt(content));
					  
				  }
				  //date
				  if (content.indexOf("<h1>DAY")>-1){
					  content=content.substring(content.indexOf(": ")+2, content.lastIndexOf("</h1>"));
					  weatherData.setDate(content); 
				  }
			  }
			  //weather
			  weathers.add(weatherData);
			  
			}catch ( Exception ex ) {
			    ex.printStackTrace();
			}
		}
		
	}
	
	public void writeToFile() throws FileNotFoundException, UnsupportedEncodingException{
		PrintWriter writer = new PrintWriter("ceren-spark.txt", "UTF-8");		
		
		for (WeatherData weatherData:weathers){
			//day,min,max,mean
			writer.println(weatherData.getDate()+", "+weatherData.getMin()+", "+weatherData.getMax()+", "+weatherData.getMean());	
		}
		writer.close();
	}
	
	public void startSpark(){
//		
		SparkConf conf = new SparkConf().setAppName("App_Name")
			    .setMaster("spark://ceren-S301LA:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		

//		
		JavaRDD<String> lines = sc.textFile("ceren-spark.txt");
		JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
		  public Integer call(String s) { 
			  String [] data= s.split(",");
			  DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			  try {
				Date result =  df.parse(data[0]);
				result.getMonth();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
			  return s.length(); }
		});
		int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer a, Integer b) { return a + b; }
		});
		System.out.println(totalLength);
	}

}
