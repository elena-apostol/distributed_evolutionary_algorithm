package ro.pub.ga.watchmaker.example.clustering;

import java.util.ArrayList;
import java.util.Random;

import org.uncommons.maths.number.ConstantGenerator;
import org.uncommons.maths.number.NumberGenerator;
import org.uncommons.maths.random.Probability;
import org.uncommons.watchmaker.framework.operators.AbstractCrossover;

public class CenterArrayListCrossover<T> extends AbstractCrossover<ArrayList<T>>
{
	int centerLength;
	
    public CenterArrayListCrossover()
    {
        this(Probability.ONE);
    }
    
    public CenterArrayListCrossover(Probability crossoverProbability)
    {
        super(2, // Requires exactly two cross-over points.
              crossoverProbability);
    }
    
    // Cu dim unui centru 
    public CenterArrayListCrossover(Probability crossoverProbability, int centerLength)
    {
        super(2, // Requires exactly two cross-over points.
              crossoverProbability);
        this.centerLength = centerLength;
    }

    public CenterArrayListCrossover(NumberGenerator<Probability> crossoverProbabilityVariable)
    {
        super(new ConstantGenerator<Integer>(2), // Requires exactly two cross-over points.
              crossoverProbabilityVariable);
    }


    @Override
    protected ArrayList<ArrayList<T>> mate(ArrayList<T> parent1,
                                 ArrayList<T> parent2,
                                 int numberOfCrossoverPoints,
                                 Random rng)
    {
 
        assert numberOfCrossoverPoints == 2 : "Expected number of cross-over points to be 2.";
        //System.out.println("Center length: " + centerLength);
        if (parent1.size() != parent2.size())
        {
            throw new IllegalArgumentException("Cannot perform cross-over with different length parents.");
        }

        ArrayList<T> offspring1 = new ArrayList<T>(parent1); // Use a random-access list for performance.
        ArrayList<T> offspring2 = new ArrayList<T>(parent2);

        int point1 = rng.nextInt(parent1.size() / centerLength);
        int point2 = rng.nextInt(parent1.size() / centerLength);

        int length = point2 - point1;
        if (length < 0)
        {
            length += parent1.size() / centerLength;
        }

        System.out.println("[Oana - Crossover]Nr of centers interchanged between parents: " + length);
        for (int i = 0; i < length; i++)
        {
            int index = (i + point1) % (parent1.size()/centerLength);
            // schimbat intre index * centerLength pana la (index + 1)*centerLength
            for (int j = index * centerLength; j < (index + 1) * centerLength; j++) {
            	T item1 = offspring1.get(j);
            	T item2 = offspring2.get(j);
            	offspring1.set(j, item2);
                offspring2.set(j, item1);
            }
        }

        ArrayList<ArrayList<T>> result = new ArrayList<ArrayList<T>>(2);
        result.add(offspring1);
        result.add(offspring2);
        
        //----------------------
        StringBuilder sb = new StringBuilder();
        sb.append("[Crossover]Parents before crossover:");
        sb.append("\n[Crossover]");
        int i = 0;
		for (T f : parent1) {
			if (i%centerLength == 0) {
				sb.append("   ");
			}
			i++;
			sb.append(f + " ");
		}
		sb.append("\n[Crossover]");
        i = 0;
		for (T f : parent2) {
			if (i%centerLength == 0) {
				sb.append("   ");
			}
			i++;
			sb.append(f + " ");
		}
		sb.append("[Crossover]Children");
		sb.append("\n[Crossover]");
		i = 0;
		for (T f : offspring1) {
			if (i%centerLength == 0) {
				sb.append("   ");
			}
			i++;
			sb.append(f + " ");
		}
		sb.append("\n[Crossover]");
		i = 0;
		for (T f : offspring2) {
			if (i%centerLength == 0) {
				sb.append("   ");
			}
			i++;
			sb.append(f + " ");
		}
		System.out.println(sb.toString());
		//----------------------
        return result;
    }

}