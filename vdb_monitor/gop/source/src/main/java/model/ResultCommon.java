package model;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ResultCommon {

	public String measure;
	public long value;
	public String tag;
	public boolean alert;

	public ResultCommon(String measure, long value,  String tag, boolean alert) {
		super();
		this.measure = measure;
		this.value = value;
		this.tag = tag;
		this.alert = alert;
	}


	public String getName() {
		return measure;
	}


	public void setName(String measure) {
		this.measure = measure;
	}


	public long getValue() {
		return value;
	}


	public void setValue(long value) {
		this.value = value;
	}


	public String getTag() {
		return tag;
	}


	public void setTag(String tag) {
		this.tag = tag;
	}


	public boolean isAlert() {
		return alert;
	}


	public void setAlert(boolean alert) {
		this.alert = alert;
	}


	public ResultCommon() {
		super();
	}


	@Override
	public String toString() {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String json = gson.toJson(this);
		
		return json;
	}
	
}
