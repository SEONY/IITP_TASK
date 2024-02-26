package service;

import java.sql.*;
import java.util.Properties;

import com.google.gson.GsonBuilder;

import model.Measure;
import model.Config;
import model.Data;
import model.JdbcProperty;
import model.ResultCommon;

public class Database {
	Config sConfig;

	public Database(Config config) {
		sConfig = config;
	}

	public Connection createConnection()
    {
        try
        {
            Class.forName(sConfig.setting.jdbcSource.driverClass);
        }
        catch (ClassNotFoundException sException)
        {
        }
		Properties prop = new Properties();
		JdbcProperty jpArr[] = sConfig.setting.jdbcSource.jdbcProperties;

		for (JdbcProperty jdbcProperty : jpArr) {
			prop.setProperty(jdbcProperty.name, jdbcProperty.value);
		}
		boolean createConnection = true;
		int i = 0;

		String dbUrl=sConfig.setting.jdbcSource.url+sConfig.setting.jdbcSource.dbName;
		while (createConnection) {
			try {	
				return DriverManager.getConnection(dbUrl, prop);
			} catch (SQLException e) {
				System.out.println("[SQLSTATE:"+ e.getSQLState() + "] getConnection error! retry con : " + i);

				i++;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
    }

	public PreparedStatement[] createConAndPstmt(Database db) {
		Connection con = null;
		PreparedStatement[] arrPstmt = null;

		do {
			con = db.createConnection();
			arrPstmt = new PreparedStatement[sConfig.measure.length];
			for (int i = 0; i < sConfig.measure.length; i++) {
				if (!sConfig.measure[i].sqlIsOs) {
					try {
						arrPstmt[i] = con.prepareStatement(sConfig.measure[i].sql);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						// e.printStackTrace();
						System.out.println("con.prepareStatement error");
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							System.out.println("[SQLSTATE:"+ e.getSQLState() + "] MSG : "+e.getMessage());
						}
					}
				}
			}
			if (arrPstmt != null) {
				return arrPstmt;
			}
		} while (arrPstmt == null);
		return arrPstmt;
	}

	public Data getCommonQuery(PreparedStatement[] arrPstmt) {

		ResultCommon[] resultArr = new ResultCommon[sConfig.measure.length];

		String sysTimestamp = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").create()
				.toJson(new Timestamp(System.currentTimeMillis()));

		for (int i = 0; i < sConfig.measure.length; i++) {

			// Statement stmt = con.createStatement();
			boolean alert = false;
			if (sConfig.measure[i].sqlIsOs) {
				long queryValue = 0;

				ReadOs oc = new ReadOs();
				queryValue = oc.execute(sConfig.measure[i].sql);

				alert = alertCheck(sConfig.measure[i], queryValue);

				resultArr[i] = new ResultCommon(sConfig.measure[i].name, queryValue, sConfig.measure[i].tag, alert);

			} else {
				ResultSet rs;
				try {
					rs = arrPstmt[i].executeQuery();

					// ResultSet rs = stmt.executeQuery(sConfig.common[i].sql);
					ResultSetMetaData rsMeta = rs.getMetaData();
					if (rsMeta.getColumnCount() != 1) {
						System.out
								.println("not support multiple column query _ Query name : " + sConfig.measure[i].name);
					}

					long queryValue = 0;
					while (rs.next()) {
						queryValue = rs.getLong(1);
						alert = alertCheck(sConfig.measure[i], queryValue);

						resultArr[i] = new ResultCommon(sConfig.measure[i].name, queryValue, sConfig.measure[i].tag,
								alert);
					}
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					System.out.println("[SQLSTATE:"+ e.getSQLState() + "] MSG : "+e.getMessage());
					System.out.println(e.getSQLState());
					e.printStackTrace();
					return null;
				}
			}
		}
		return new Data(sysTimestamp, resultArr);
	}

	private boolean alertCheck(Measure common, long queryValue) {
		switch (common.alertPolicy) {
		case 1:
			if (common.alertValue < queryValue) {
				return true;
			}
			break;
		case 2:
			if (common.alertValue > queryValue) {
				return true;
			}
			break;
		case 3:
			if (common.alertValue == queryValue) {
				return true;
			}
			break;
		default:
			return false;
		}

		return false;
	}

}