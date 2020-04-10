package ro.pub.ga.watchmaker.jss;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.uncommons.maths.number.NumberGenerator;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;

public class DoubleListMutation<T> implements EvolutionaryOperator<List<List<T>>>{

	
	private NumberGenerator<Integer> mutationCountVariable;
	private NumberGenerator<Integer> mutationAmountVariable;
	private double mutationRate;
	
	public DoubleListMutation(NumberGenerator<Integer> mutationCount,
	            NumberGenerator<Integer> mutationAmount, double mutationRate) {
		
		this.mutationCountVariable = mutationCount;
		this.mutationAmountVariable = mutationAmount;
		this.mutationRate = mutationRate;
	}
	
	@Override
	public List<List<List<T>>> apply(List<List<List<T>>> selectedCandidates,
			Random rng) {
		
		int mutationCount, mutationAmount;
		int fromIndex, toIndex;
		
		List<List<List<T>>> mutants = new ArrayList<List<List<T>>>(selectedCandidates.size());
		
		for(List<List<T>> candidate: selectedCandidates) {
			
			List<List<T>> newCandidate = getCopyOfCandidate(candidate);
			
			if(rng.nextDouble() < mutationRate) {
				
				for(List<T> chromosome: newCandidate) {
					
					mutationCount = mutationCountVariable.nextValue();
					//System.out.println("Mutation Count: " + mutationCount);
					for(int i = 0; i < mutationCount; i++) {
						
						mutationAmount = mutationAmountVariable.nextValue();
						//System.out.println("\tMutation Amount: " + mutationAmount);
						fromIndex = rng.nextInt(chromosome.size());
						toIndex = (fromIndex + mutationAmount) % chromosome.size();
						Collections.swap(chromosome, fromIndex, toIndex);
					}
				}
			}
			
			mutants.add(newCandidate);
		}
		
		return mutants;
	}
	
	public List<List<T>> getCopyOfCandidate(List<List<T>> candidate) {
		
		List<List<T>> newCandidate = new ArrayList<List<T>>(candidate.size());
		for(int i = 0; i < candidate.size(); i++) {
			
			newCandidate.add(new ArrayList<T>(candidate.get(i)));
		}
		
		return newCandidate;
	}

}
