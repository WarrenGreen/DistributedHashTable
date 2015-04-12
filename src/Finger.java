package src;

public class Finger {
	private int start;
	private int[] interval;
	private int successor;
	
	public Finger() {
		
	}
	
	public Finger(int start, int[] interval) {
		this.start = start;
		this.interval = interval;
	}
	
	public Finger(int start, int[] interval, int successor) {
		this.start = start;
		this.interval = interval;
		this.successor = successor;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getSuccessor() {
		return successor;
	}

	public void setSuccessor(int successor) {
		this.successor = successor;
	}
	
	public int getIntervalStart() {
		return interval[0];
	}
	
	public int getIntervalEnd() {
		return interval[1];
	}
	
	public void setInterval(int start, int end) {
		this.interval = new int[]{start, end};
	}

	public int[] getInterval() {
		return interval;
	}

	public void setInterval(int[] interval) {
		this.interval = interval;
	}

}
