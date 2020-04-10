package ro.pub.ga.watchmaker.utils;

import java.io.Serializable;
import java.util.Comparator;

import ro.pub.ga.watchmaker.core.Constants;

public class PopulationWritableComparator implements Comparator<PopulationWritable<Serializable>>{

	@Override
	public int compare(PopulationWritable<Serializable> o1,
			PopulationWritable<Serializable> o2) {
		
		Integer index1 = (Integer)o1.getDataValue(Constants.POPULATION_INDEX_PARAMETER);
		Integer index2 = (Integer)o2.getDataValue(Constants.POPULATION_INDEX_PARAMETER);
		
		//Intentionally avoided null checks; it is a non-recoverable error;
		
		if(index1 == index2) {
			
			return 0;
		} 
		else {
			
			if(index1 > index2) {
				
				return 1;
			}
			else {
				
				return -1;
			}
		}
	}

}
