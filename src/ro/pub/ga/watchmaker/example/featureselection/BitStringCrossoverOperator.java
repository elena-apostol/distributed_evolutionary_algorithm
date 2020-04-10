package ro.pub.ga.watchmaker.example.featureselection;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.uncommons.maths.binary.BitString;
import org.uncommons.watchmaker.framework.operators.AbstractCrossover;
import org.uncommons.watchmaker.framework.operators.BitStringCrossover;

public class BitStringCrossoverOperator extends AbstractCrossover<BitString> {

	public enum CrossoverOperatorType {
    	AND, OR, XOR, BitStringCrossover
    };
    
    public static AbstractCrossover<BitString> getOperator(int crossoverPoints, CrossoverOperatorType type) {
    	switch (type) {
    	case BitStringCrossover:
    		return new BitStringCrossover();
    	default :
    		return new BitStringCrossoverOperator(crossoverPoints, type);
    	}
    }
    
    private CrossoverOperatorType type;
    
    private BitStringCrossoverOperator(int crossoverPoints, CrossoverOperatorType type) {
		super(crossoverPoints);
		this.type = type;
	}
    
    @Override
	protected List<BitString> mate(BitString arg0, BitString arg1, int arg2,
			Random arg3) {
		if (arg0.getLength() != arg1.getLength()) {
			throw new IllegalArgumentException("Cannot perform cross-over with different length parents.");
		}
		
		BitString offspring = new BitString(arg0.getLength());
		for (int i = 0; i < offspring.getLength(); i++) {
			offspring.setBit(i, applyOp(arg0.getBit(i), arg1.getBit(i)));
		}
		List<BitString> result = new ArrayList<BitString>();
		result.add(offspring);
		return result;
	}
    
    private boolean applyOp(boolean bit1, boolean bit2) {
    	switch(this.type) {
    	case AND:
    		return bit1 & bit2;
    	case OR:
    		return bit1 | bit2;
    	case XOR:
    		return bit1 ^ bit2;
    	default:
    		return false;
    	}
    }
}
