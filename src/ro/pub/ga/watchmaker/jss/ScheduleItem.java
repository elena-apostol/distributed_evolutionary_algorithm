package ro.pub.ga.watchmaker.jss;

public class ScheduleItem {
	
	private int taskNumber;
	private int jobNumber;
	private int  machineNumber;
	private long start;
	private long end;
	
	

	public ScheduleItem(int taskNumber, int jobNumber, int machineNumber) {

		this.taskNumber = taskNumber;
		this.jobNumber = jobNumber;
		this.machineNumber = machineNumber;
		this.start = -1;
		this.end = -1;
	}

	public ScheduleItem(int taskNumber, int jobNumber, int machineNumber, long start, long end) {
		
		this.taskNumber = taskNumber;
		this.jobNumber = jobNumber;
		this.machineNumber = machineNumber;
		this.start = start;
		this.end = end;
	}
	
	public ScheduleItem(ScheduleItem si) {
		
		this.taskNumber = si.getTaskNumber();
		this.jobNumber = si.getJobNumber();
		this.machineNumber = si.getMachineNumber();
		this.start = si.getStart();
		this.end = si.getEnd();
	}
	
	public int getTaskNumber() {
		return taskNumber;
	}
	public void setTaskNumber(int taskNumber) {
		this.taskNumber = taskNumber;
	}
	public int getJobNumber() {
		return jobNumber;
	}
	public void setJobNumber(int jobNumber) {
		this.jobNumber = jobNumber;
	}
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}

	public int getMachineNumber() {
		return machineNumber;
	}

	public void setMachineNumber(int machineNumber) {
		this.machineNumber = machineNumber;
	}

	@Override
	public String toString() {
		
		return "(t" + taskNumber + ", j" + jobNumber + ", m" + machineNumber + ", " + start + ", " + end + ")"; 
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (end ^ (end >>> 32));
		result = prime * result + jobNumber;
		result = prime * result + machineNumber;
		result = prime * result + (int) (start ^ (start >>> 32));
		result = prime * result + taskNumber;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ScheduleItem other = (ScheduleItem) obj;
		if (end != other.end)
			return false;
		if (jobNumber != other.jobNumber)
			return false;
		if (machineNumber != other.machineNumber)
			return false;
		if (start != other.start)
			return false;
		if (taskNumber != other.taskNumber)
			return false;
		return true;
	}
	
	
}