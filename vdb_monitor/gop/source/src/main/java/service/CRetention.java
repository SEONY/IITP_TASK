package service;

import java.io.File;
import java.io.FilenameFilter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CRetention {

	ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
	public static String path = "";
    public static int p_day = 0;
	public static final String YYYYMMDD = "(19|20)\\d{2}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])";
    public static SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

	public CRetention(String log_path) {
		path = log_path;
	}
	
	public void go(String period) {
		//주기는 분 또는 시간으로 올수 있으니 unit은 변경 필요
        p_day = Integer.parseInt(period);
		service.scheduleAtFixedRate(t_runer, 0, 1, TimeUnit.DAYS);
	}
	
	public void stop() {
		service.isTerminated();
	}
	
	Runnable t_runer= new Runnable() {
		public void run() {
            ArrayList<File> deleteTarget = new ArrayList<>();
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-(p_day));
            Date today = cal.getTime();
            System.out.println(formatter.format(today));

			File dir = new File(path);
            //filefilter for alert
            FilenameFilter filter1 = new FilenameFilter() {
                public boolean accept(File f, String name) {
                    return name.startsWith("alert_");
                }
            };
            //filefilter for logs
            FilenameFilter filter2 = new FilenameFilter() {
                public boolean accept(File f, String name) {
                    return name.startsWith("log_");
                }
            };
			File alerts[] = dir.listFiles(filter1);
            File logs[] = dir.listFiles(filter2);

			for (int i = 0; i < alerts.length; i++) {
                Pattern pattern = Pattern.compile(YYYYMMDD);
                Matcher matcher = pattern.matcher(alerts[i].getName());
                
                while(matcher.find()){
                    try {
                        Date t = formatter.parse(matcher.group());
                        if((t.compareTo(today)) <= 0)
                        {
                            deleteTarget.add(alerts[i]);
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
			}

            for (int i = 0; i < logs.length; i++) {
                Pattern pattern = Pattern.compile(YYYYMMDD);
                Matcher matcher = pattern.matcher(logs[i].getName());
                
                while(matcher.find()){
                    try {
                        Date t = formatter.parse(matcher.group());
                        if(t.compareTo(today) <= 0)
                        {
                            deleteTarget.add(logs[i]);
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
			}

            for(int i = 0; i < deleteTarget.size(); i++) {
                System.out.println("file: " + deleteTarget.get(i).getName());
                File a = new File(deleteTarget.get(i).getAbsolutePath());
                a.delete();
            }
		}
	};

}
