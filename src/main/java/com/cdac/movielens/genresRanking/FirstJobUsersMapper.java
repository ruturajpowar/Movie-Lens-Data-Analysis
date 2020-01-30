package com.cdac.movielens.genresRanking;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstJobUsersMapper extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();
	private Map<String ,String> occupationMap=new HashMap<String, String>();
	
	@Override
	public void setup(Context context) {
		occupationMap.put("0", "other");
		occupationMap.put("1","academic/educator");
		occupationMap.put("2","artist");
		occupationMap.put("3","clerical/admin");
		occupationMap.put("4", "college/grad student");
		occupationMap.put("5","customer service");
		occupationMap.put("6","doctor/health care");
		occupationMap.put("7","executive/managerial");
		occupationMap.put("8","farmer");
		occupationMap.put("9","homemaker");
		occupationMap.put("10", "K-12 student");
		occupationMap.put("11","lawyer");
		occupationMap.put("12","programmer");
		occupationMap.put("13","retired");
		occupationMap.put("14","sales/marketing");
		occupationMap.put("15","scientist");
		occupationMap.put("16","self-employed");
		occupationMap.put("17", "technician/engineer");
		occupationMap.put("18","tradesman/craftsman");
		occupationMap.put("19","unemployed" );
		occupationMap.put("20", "writer");
		
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] usersData = value.toString().split("::");
		if (usersData != null && usersData.length == 5 && usersData[0].length() > 0) {
			Optional<String> ageRange = getAgeRange(usersData[2]);
			if (ageRange.isPresent()) {
				outKey.set(usersData[0]);
				outValue.set("U" + ageRange.get() + "::" + occupationMap.get(usersData[3]));
				context.write(outKey, outValue);
			}
		}
	}

	private Optional<String> getAgeRange(String userAge) {
		int age = Integer.parseInt(userAge);
		if (age < 18) {
			return Optional.empty();
		} else if (age >= 18 && age <= 35) {
			return Optional.of("18-35");
		} else if (age >= 36 && age <= 50) {
			return Optional.of("36-50");
		} else if (age > 50) {
			return Optional.of("50+");
		}
		return Optional.empty();
	}
}



