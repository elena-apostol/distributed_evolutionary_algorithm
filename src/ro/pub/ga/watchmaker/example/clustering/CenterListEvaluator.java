package ro.pub.ga.watchmaker.example.clustering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.uncommons.watchmaker.framework.FitnessEvaluator;

public class CenterListEvaluator implements FitnessEvaluator<List<Float>> {
	private List<ArrayList<Float>> pointsSet;
	private int nrOfCenters;
	private boolean modifyCandidateFlag = false;
	
	public CenterListEvaluator(List<ArrayList<Float>> pointsSet, int nrOfCenters) {
		this.pointsSet = pointsSet;
		this.nrOfCenters = nrOfCenters;
	}
	
	@Override
	public double getFitness(List<Float> candidate,
			List<? extends List<Float>> population) {
		
		ArrayList<HashSet<ArrayList<Float>>> pointAssignment = new ArrayList<HashSet<ArrayList<Float>>>();
		for (int i = 0; i < nrOfCenters; i++) {
			pointAssignment.add(new HashSet<ArrayList<Float>>());
		}
		
		// create arrayList with centers
		ArrayList<ArrayList<Float>> centers = new ArrayList<ArrayList<Float>>();
		int dim = candidate.size() / nrOfCenters;
		for (int i = 0; i < nrOfCenters; i++) {
			ArrayList<Float> center = new ArrayList<Float>();
			for (int j = i * dim; j < (i + 1) * dim; j++) {
				center.add(new Float(candidate.get(j).floatValue()));
			}
			centers.add(center);
		}

		//---------------------
		StringBuilder sb = new StringBuilder();
        sb.append("[Fitness]Fitness for:");
        sb.append("\n[Fitness]");
        int k = 0;
		for (Float f : candidate) {
			if (k%dim == 0) {
				sb.append("   ");
			}
			k++;
			sb.append(f + " ");
		}
		//---------------------
		
		// asigneaza fiecare punct din pointsSet unui centru - cel mai aproapiat ca distanta
		//pt fiecare centru voi crea un hashSet
		for (int j = 0; j < pointsSet.size(); j++) {
			ArrayList<Float> point = pointsSet.get(j);
			// cauta cel mai apropiat centru existent 
			Float minDistance = Float.MAX_VALUE;
			int centerIndex = 0;
			for (int i = 0; i < centers.size(); i++) {
				ArrayList<Float> center = centers.get(i);
				Float distance = getDistance(center, point);
				if (distance < minDistance) {
					minDistance = distance;
					centerIndex = i;
				}
			}
			pointAssignment.get(centerIndex).add(point);
		}
		
		// calculeaza si seteaza noile centre ca medie aritmetica
		// folosind valorile din hashset si din candidate
		for (int i = 0; i < centers.size(); i++) {
			for (int d = 0; d < dim; d++) {
				Float avg = new Float(centers.get(i).get(d));
				for (ArrayList<Float> point : pointAssignment.get(i)) {
					avg += point.get(d);
				}
				avg = avg / (pointAssignment.get(i).size() + 1);
				// update in candidate and centers (in centers because we use later for fitness computation
				if (modifyCandidateFlag) {
					candidate.set(i * dim + d, avg);
				}
				centers.get(i).set(d, avg);
			}
		}
		
		// calculeaza fitness ca suma distantelor pt fiecare centru la pc asignate lui
		Float fitness = new Float(0);
		for (int i = 0; i < centers.size(); i++) {
			ArrayList<Float> center = centers.get(i);
			for (ArrayList<Float> point : pointAssignment.get(i)) {
				fitness += getDistance(center, point);
			}
		}
		/*System.out.println("Fitness " + fitness);
		*/
		
		sb.append(" => " + fitness);
		//System.out.println(sb.toString());
		
		return fitness;
	}
	
	public double getFitnessAndUpdateCandidate(List<Float> candidate,
			List<? extends List<Float>> population) {
		modifyCandidateFlag = true;
		double fitness = getFitness(candidate, population);
		modifyCandidateFlag = false;
		return fitness;
	}
	
	public Float getDistance(ArrayList<Float> p1, ArrayList<Float> p2) {
		float distance = 0;
		for (int i = 0; i < p1.size(); i++) {
			distance += ((p1.get(i) - p2.get(i)) * (p1.get(i) - p2.get(i)));
		}
		return new Float(Math.sqrt(distance));
	}

	@Override
	public boolean isNatural() {
		return false;
	}
}
