package edu.umich.robustopt.metering;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.umich.robustopt.clustering.WeightedQuery;


public class DesignExecutionTrace implements Serializable {

	private Date date;
	private long totalTimeInSeconds;
	private String message;
	
	private static final long serialVersionUID = 660308389166411723L;

	public DesignExecutionTrace(Date date, long totalTimeInSeconds, String message) {
		this.date = date;
		this.totalTimeInSeconds = totalTimeInSeconds;
		this.message = message;
	}
	public DesignExecutionTrace(long totalTimeInSeconds, String message) {
		this(new Date(), totalTimeInSeconds, message);
	}
	
	@Override
	public String toString() {
		SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
		String str = "Total Time=" + totalTimeInSeconds/60 + " mins. Date: " + ft.format(date) + "\n" + message + "\nEND OF MESSAGE";
		return str;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + ((message == null) ? 0 : message.hashCode());
		result = prime * result
				+ (int) (totalTimeInSeconds ^ (totalTimeInSeconds >>> 32));
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
		DesignExecutionTrace other = (DesignExecutionTrace) obj;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (message == null) {
			if (other.message != null)
				return false;
		} else if (!message.equals(other.message))
			return false;
		if (totalTimeInSeconds != other.totalTimeInSeconds)
			return false;
		return true;
	}
	public Date getDate() {
		return date;
	}
	public long getTotalTimeInSeconds() {
		return totalTimeInSeconds;
	}
	public String getMessage() {
		return message;
	}

	
	
}
