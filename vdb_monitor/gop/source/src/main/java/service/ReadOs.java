package service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ReadOs {
	int execute(String cmd) {
		return Integer.parseInt(runS(cmd));
	}

	public static String executeS(String cmd) {
		return runS(cmd);
	}

	private static String runS(String cmd) {
		Process process = null;
		Runtime runtime = Runtime.getRuntime();
		StringBuffer successOutput = new StringBuffer();
		StringBuffer errorOutput = new StringBuffer();
		BufferedReader successBufferReader = null;
		BufferedReader errorBufferReader = null;
		String msg = null;

		List<String> cmdList = new ArrayList<String>();

		if (System.getProperty("os.name").indexOf("Linux") > -1) {
			cmdList.add("/bin/sh");
			cmdList.add("-c");
		}

		cmdList.add(cmd);
		String[] array = cmdList.toArray(new String[cmdList.size()]);
		// System.out.println(array[0]);

		try {

			// 명령어실행
			process = runtime.exec(array);

			// shell실행이정상동작했을경우
			successBufferReader = new BufferedReader(new InputStreamReader(process.getInputStream(), "EUC-KR"));

			while ((msg = successBufferReader.readLine()) != null) {
				successOutput.append(msg + System.getProperty("line.separator"));
			}

			// shell실행시에러가발생했을경우
			errorBufferReader = new BufferedReader(new InputStreamReader(process.getErrorStream(), "EUC-KR"));
			while ((msg = errorBufferReader.readLine()) != null) {
				errorOutput.append(msg + System.getProperty("line.separator"));
			}

			// 프로세스의수행이끝날때까지대기
			process.waitFor();

			// shell실행이정상종료되었을경우
			if (process.exitValue() == 0) {
				// System.out.println("성공");
				msg = successOutput.toString().trim();
				// System.out.println(msg);
			} else {
				// shell실행이비정상종료되었을경우
				System.out.println("비정상종료: " + cmd);
				System.out.println(successOutput.toString());
			}

			// shell실행시에러가발생
			// if (CommonUtil.notEmpty(errorOutput.toString())) {
			// // shell실행이비정상종료되었을경우
			// System.out.println("오류");
			// System.out.println(successOutput.toString());
			// }

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			try {
				process.destroy();
				if (successBufferReader != null)
					successBufferReader.close();
				if (errorBufferReader != null)
					errorBufferReader.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		return msg;
	}
}
