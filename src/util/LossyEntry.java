package util;


import java.io.Serializable;

public class LossyEntry implements Comparable<LossyEntry>, Serializable{

	private String element;
	private int frequency;
	private int delta;
	private int svalue;
	
	public LossyEntry(String element, int frequency, int delta, int svalue){
		this.element = element;
		this.frequency = frequency;
		this.delta = delta;
		this.svalue = svalue;
		
	}
	
	public String getElement(){
		return element;
	}
	
	public int getFrequency(){
		return frequency;
	}
	
	public int getDelta(){
		return delta;
	}
	
	public int getSvalue(){
		return svalue;
	}

	public void setFrequency(int frequency){
		this.frequency = frequency;
	}
	
	public void setSvalue(int svalue){
		this.svalue = svalue;
	}
	
	@Override
	public int compareTo(LossyEntry o) {
		Integer f = new Integer(frequency);
		return f.compareTo(o.getFrequency());
	}
	
	public boolean equals(Object obj){
		if(!(obj instanceof LossyEntry))
			return false;
		else
			return element.equals(((LossyEntry) obj).getElement());
	}
	
	public String toString(){
		return "<" + element + ":s=" + svalue/frequency + ",f=" + frequency + ">";
	}
	
}
