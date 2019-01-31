package mentoring.pagerank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import mentoring.pagerank.SqlClean.SqlMapper;

public class a {

	public static class Test {

		static String data = "123	100	ns0";

		public String map(String a) throws IOException {
			System.out.println("-----");
			String[] parts = StringUtils.splitPreserveAllTokens(data.toString(), "\t");
			
//			Properties props = new Properties();
//			String fileName = "NS.properties";
//			FileInputStream inputStream;
//			inputStream = new FileInputStream(fileName);
//			InputStreamReader reader = new InputStreamReader(inputStream, "UTF-8");
//			props.load(reader);
			
			Properties props = new Properties();
			InputStream proFile = SqlMapper.class.getResourceAsStream("NS.properties");
			InputStreamReader reader = new InputStreamReader(proFile, "UTF-8");
			props.load(reader);
			
			
			
			String id = parts[0];
			String nameSpace = parts[1];
			String title = parts[2];
			String newData = null;

			String ns = props.getProperty(nameSpace);
			System.out.println(ns);
			if (ns.isEmpty()) {
				newData = title;
			} else {
				newData = ns + ":" + title;
			}
			return id + "\t" + newData;
			// return nameSpace;
		}
//		public String toString() {
//			return id + newData;
//		}
	}

	public static void main(String[] args) throws Exception {
		Test t = new Test();
		String d = t.map("123	11	ns0");
		System.out.println(d);
		//System.out.println(t.data);
		System.out.println(new File(".").getAbsolutePath());
	}

}
