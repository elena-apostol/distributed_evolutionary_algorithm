package ro.pub.ga.watchmaker.example.clustering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.uncommons.maths.number.NumberGenerator;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;

public class CenterArrayListMutation<T> implements EvolutionaryOperator<ArrayList<T>> {

	private final NumberGenerator<Integer> mutationCountVariable;
    private final NumberGenerator<Integer> mutationAmountVariable;
    
	public CenterArrayListMutation(NumberGenerator<Integer> mutationCount,
            NumberGenerator<Integer> mutationAmount) {
		this.mutationCountVariable = mutationCount;
		this.mutationAmountVariable = mutationAmount;
	}

	
	@Override
	public List<ArrayList<T>> apply(List<ArrayList<T>> selectedCandidates,
			Random rng) {
		
		ArrayList<ArrayList<T>> result = new ArrayList<ArrayList<T>>(selectedCandidates.size());
        for (ArrayList<T> candidate : selectedCandidates)
        {
            ArrayList<T> newCandidate = new ArrayList<T>(candidate);
            //-----------
            StringBuilder sb = new StringBuilder();
            sb.append("[Mutation]Before mutation:");
            sb.append("\n[Mutation]");
    		for (T f : candidate) {
    			sb.append(f + " ");
    		}
    		//-------------
    		int mutationCount = Math.abs(mutationCountVariable.nextValue());
            for (int i = 0; i < mutationCount; i++)
            {
                // TODO 1 is exclusive - in articol e inclusiv
                Integer index = new Random().nextInt(newCandidate.size());
                // Il modifica prea mult!
                Float modifiedValue = (new Random().nextFloat()) / 2;
                Float v = (Float)newCandidate.get(index);
                if (v != 0.0) {
                	modifiedValue *= v;
                }
                // + sau -
                modifiedValue = (Math.random() <= 0.5) ? modifiedValue : - modifiedValue;
                modifiedValue = v + modifiedValue;
                newCandidate.set(index, (T)modifiedValue);
            }
            //-----------
            sb.append("[Mutation]After mutation:");
            sb.append("\n[Mutation]");
    		for (T f : newCandidate) {
    			sb.append(f + " ");
    		}
    		System.out.println(sb.toString());
    		//------------
            result.add(newCandidate);
        }
        return result;
	}

}
