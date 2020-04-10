package ro.pub.ga.watchmaker.example.clustering;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.uncommons.watchmaker.framework.factories.AbstractCandidateFactory;

public class CenterCandidateFactory<T> extends AbstractCandidateFactory<ArrayList<T>> {

	private List<ArrayList<T>> points;
	private int nrOfCenters;
	
	public CenterCandidateFactory(List<ArrayList<T>> points, int nrOfCenters) {
		this.points = points;
		this.nrOfCenters = nrOfCenters;
	}
	
	@Override
	public ArrayList<T> generateRandomCandidate(Random rng) {
		HashSet<Integer> usedIndexes = new HashSet<Integer>();
		ArrayList<T> candidate = new ArrayList<T>();
		for (int i = 0 ; i < nrOfCenters; i++) {
			Integer index = rng.nextInt(points.size());
			while (usedIndexes.contains(index))
				index = rng.nextInt(points.size());
			// indexul trebuie sa fie diferit - sa nu am doua centre la fel in candidat!
			candidate.addAll(points.get(index));
			usedIndexes.add(index);
		}
		return candidate;
	}
}
