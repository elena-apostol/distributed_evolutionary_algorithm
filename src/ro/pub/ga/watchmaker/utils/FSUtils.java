package ro.pub.ga.watchmaker.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FSUtils {
	
	private final FileSystem fs;
	
	private FSUtils() {
		
		this.fs = null;
	}
	
	public FSUtils(FileSystem fs) {
		
		this.fs = fs;
	}
	 
	 public void writeToFile(String pathToFile, String data) {
		 
		 try {
			 
			Path path = new Path(pathToFile);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
			writer.write(data);
			writer.flush();
			writer.close();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	 }
	 
	 public String readFromFile(String pathToFile) {
		 
		 StringBuilder data = new StringBuilder("");
		 String line = null;
		 
		 try {
			 
			 Path path = new Path(pathToFile);
		     BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(path)));
		     
		     while((line = reader.readLine()) != null) {
		    	 // Oana changed - added - '+ "\n"' - there was bug
		    	 data.append(line + "\n");
		     }
		     
		     reader.close();
		 }
		 catch(Exception e) {
			 
			 e.printStackTrace();
		 }

		 return data.toString();
	 }
	 
	 public void mkdir(String directory) {
		 
		 Path path = new Path(directory);
		 try {
			 
			if(fs.exists(path)) {
				 
				 fs.delete(path, true);
			 }
			
			fs.mkdirs(path);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	 }
	 
	 public void mkdir(String directory, boolean deleteIfExists) {
		 
		 Path path = new Path(directory);
		 try {
			 
			if(fs.exists(path)) {
				 
				if(deleteIfExists) {
					
					fs.delete(path, true);
				}
				else {
					
					return;
				}
			 }
			
			fs.mkdirs(path);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	 }
	 
	 public void delete(String directory) {
		 
		 Path path = new Path(directory);
		 try {
			 
			if(fs.exists(path)) {
				 
				 fs.delete(path, true);
			 }
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		 
	 }
}
