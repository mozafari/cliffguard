package com.relationalcloud.tsqlparser.schema.datatypes;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class TimestampDataType {

	boolean currentTime;
	int year;
	int month;
	int day;
	int hour;
	int minute;
	int second;
	String millis;
	
	public String getMillis() {
		return millis;
	}

	public void setMillis(String millis) {
		this.millis = millis;
	}

	public void setCurrentTime(boolean t){
		currentTime=t;
	}
	
	public boolean isCurrentTime(){
		return currentTime;
	}
	
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public int getDay() {
		return day;
	}
	public void setDay(int day) {
		this.day = day;
	}
	public int getHour() {
		return hour;
	}
	public void setHour(int hour) {
		this.hour = hour;
	}
	public int getMinute() {
		return minute;
	}
	public void setMinute(int minute) {
		this.minute = minute;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}
	public String toString()
	{
		if(currentTime)
		{
			Date d = new Date();
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			return df.format(d);
		}
		DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date;
		try 
		{
			date = (Date)formatter.parse(year +"-" +month +"-" + day + " " + hour+":"+minute+":"+second);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return df.format(date)+"."+millis;				
		} 
		catch (ParseException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	public String toDecimalTime()
	{
		if(currentTime)
		{
			Date d = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
			return df.format(d)+"000";
		}
		DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date;
		try 
		{
			date = (Date)formatter.parse(year +"-" +month +"-" + day + " " + hour+":"+minute+":"+second);
			DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
			return df.format(date)+"."+millis;				
		} catch (ParseException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}	
	}
}