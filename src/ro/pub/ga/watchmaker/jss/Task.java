package ro.pub.ga.watchmaker.jss;

public class Task {

	private int machineNumber;
	private long executionTime;
	
	public Task(int machineNumber, long executionTime) {
		
		this.machineNumber = machineNumber;
		this.executionTime = executionTime;
	}
	
	public int getMachineNumber() {
		return machineNumber;
	}
	public void setMachineNumber(int machineNumber) {
		this.machineNumber = machineNumber;
	}
	public long getExecutionTime() {
		return executionTime;
	}
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}

	@Override
	public String toString() {
		
		return "(" + machineNumber + ", " + executionTime + ")";
	}
	
	
}
