package model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Data {
	public String time;
	public ResultCommon rc[];

	public Data(String time, ResultCommon[] resultCommons) {
		super();
		this.time = time;
		this.rc = resultCommons;
	}

	public Data(Data data) {
		// TODO Auto-generated constructor stub
		this.time = data.time;
		ResultCommon[] rc = data.rc.clone();
		// new ResultCommon[data.rc.length];

		for (int i = 0; i < data.rc.length; i++) {
			rc[i] = new ResultCommon(data.rc[i].measure, data.rc[i].value, data.rc[i].tag, data.rc[i].alert);
		}
		this.rc = rc;
	}

	public Data newInstance(Data data) {
		return new Data(data);
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public ResultCommon[] getRc() {
		return rc;
	}

	public void setRc(ResultCommon[] rc) {
		this.rc = rc;
	}

	@Override
	public String toString() {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String json = gson.toJson(this);

		return json;
	}

}
