package ro.pub.ga.watchmaker.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.uncommons.watchmaker.framework.factories.AbstractCandidateFactory;

/**
 * Generates random candidates from a set of elements.  Each candidate is a random
 * permutation of the full set of elements.
 * @param <T> The component type of the lists created by this factory.
 * @author Daniel Dyer
 */
public class ArrayListPermutationFactory<T>  extends AbstractCandidateFactory<ArrayList<T>>
{
    private final ArrayList<T> elements;

    /**
     * Creates a factory that creates lists that contain each of the specified
     * elements exactly once.  The ordering of those elements within generated
     * lists is random.
     * @param elements The elements to permute.
     */
    public ArrayListPermutationFactory(ArrayList<T> elements)
    {
        this.elements = elements;
    }


    /**
     * Generates a random permutation from the configured elements.
     * @param rng A source of randomness used to generate the random
     * permutation.
     * @return A random permutation.
     */
    public ArrayList<T> generateRandomCandidate(Random rng)
    {
        ArrayList<T> candidate = new ArrayList<T>(elements);
        Collections.shuffle(candidate, rng);
        return candidate;
    }
}
