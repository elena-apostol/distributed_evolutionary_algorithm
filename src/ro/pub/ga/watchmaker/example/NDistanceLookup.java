package ro.pub.ga.watchmaker.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.ga.watchmaker.travellingsalesman.DistanceLookup;

public class NDistanceLookup implements DistanceLookup{
	
	private int[][] distances;
	private List<String> knownCities;
	
	private NDistanceLookup() {
		
	}
	
	public NDistanceLookup(String inputFile) {
		
		  init(inputFile, Integer.MAX_VALUE);
		  
	}
	
	public NDistanceLookup(String inputFile, int numberOfCities) {
		
		init(inputFile, numberOfCities);
	}
	
	private void init(String inputFile, Integer numberOfCities) {
		
		if(numberOfCities == null) {
			
			numberOfCities = Integer.MAX_VALUE;
		}
		
		try {
			
			  distances = TSPFileParser.parseFile(inputFile);
		  } catch (TSPException e) {
					
			  e.printStackTrace();
		  }
		  
		  knownCities = new ArrayList<String>(distances.length);
		  for(int i = 0; i < Math.min(distances.length, numberOfCities); i++) {
			  
			  knownCities.add(i + "");
		  }
		  
		  System.out.println("[NDistanceLookup] Number of cities read from " + inputFile + ": " + distances.length);
	}

	@Override
	public List<String> getKnownCities() {
		
		return knownCities;
	}

	@Override
	public int getDistance(String startingCity, String destinationCity) {
		
		int start = Integer.parseInt(startingCity);
		int destination = Integer.parseInt(destinationCity);
		
		return distances[start][destination];
	}

}
