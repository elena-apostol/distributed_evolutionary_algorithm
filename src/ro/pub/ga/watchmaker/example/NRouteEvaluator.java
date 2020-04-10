package ro.pub.ga.watchmaker.example;

import java.util.List;

import org.apache.mahout.ga.watchmaker.travellingsalesman.DistanceLookup;
import org.uncommons.watchmaker.framework.FitnessEvaluator;

public class NRouteEvaluator implements FitnessEvaluator<List<String>> {
	
	private DistanceLookup distanceLookup;

	public NRouteEvaluator(DistanceLookup distanceLookup) {
		super();
		
		this.distanceLookup = distanceLookup;
	}

	@Override
	public double getFitness(List<String> candidate,
			List<? extends List<String>> population) {
		int totalDistance = 0;
		
		for(int i = 0; i < candidate.size() - 1; i++) {
			
			totalDistance += distanceLookup.getDistance(candidate.get(i), candidate.get(i + 1));
		}
		
		return totalDistance;
	}

	@Override
	public boolean isNatural() {
		
		return false;
	}

}
