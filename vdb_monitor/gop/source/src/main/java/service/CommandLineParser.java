package service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CommandLineParser {
	List<String> args = new ArrayList<>();
	HashMap<String, List<String>> map = new HashMap<>();
	Set<String> flags = new HashSet<>();

	public CommandLineParser(String arguments[]) {
		this.args = Arrays.asList(arguments);
		map();
	}

	// Return argument names
	public Set<String> getArgumentNames() {
		Set<String> argumentNames = new HashSet<>();
		argumentNames.addAll(flags);
		argumentNames.addAll(map.keySet());
		return argumentNames;
	}

	// Check if flag is given
	public boolean getFlag(String flagName) {
		if (flags.contains(flagName))
			return true;
		return false;
	}

	public int getArgumentValueInt(String argumentName) {
		if (getArgumentValue(argumentName)[0] != null) {
			return Integer.parseInt(getArgumentValue(argumentName)[0]);
		} else {
			return 0;
		}
	}

	// Return argument value for particular argument name
	public String[] getArgumentValue(String argumentName) {
		if (map.containsKey(argumentName))
			return map.get(argumentName).toArray(new String[0]);
		else
			return new String[2];
	}

	// Map the flags and argument names with the values
	public void map() {
		for (String arg : args) {
			if (arg.startsWith("-")) {
				if (args.indexOf(arg) == (args.size() - 1)) {
					flags.add(arg.replace("-", ""));
				} else if (args.get(args.indexOf(arg) + 1).startsWith("-")) {
					flags.add(arg.replace("-", ""));
				} else {
					// List of values (can be multiple)
					List<String> argumentValues = new ArrayList<>();
					int i = 1;
					while (args.indexOf(arg) + i != args.size() && !args.get(args.indexOf(arg) + i).startsWith("-")) {
						argumentValues.add(args.get(args.indexOf(arg) + i));
						i++;
					}
					map.put(arg.replace("-", ""), argumentValues);
				}
			}
		}
	}

    public void printHelp() {
		System.out.println(" ---");
		System.out.println(" -config <config file path> [ -demon | -client -log <log file path> <option> ]");
		System.out.println(" ");
		System.out.println(" client option:");
		System.out.println(
				"   -log <log file path> [ -time 'yyyy-mm-dd hh24:mi:ss.fff' 'yyyy-mm-dd hh24:mi:ss.fff' | -name <column name> | -tag <tag name> ]");
		System.out.println("			[ -head | -tail <print count> ]  ");
		System.out.println(" ");
		System.out.println(" ---");
		System.out.println(" sample use");
		System.out.println(" java -Xmx100M -jar gop.jar -config resource/config.json -demon  ");
		System.out.println(
				" java -jar gop.jar -config resource/config.json -client -log resource/log_20221201.json -time '2022-12-01 03:14:40.000' '2022-12-01 03:15:00.000'");
		System.out.println(
				" java -jar gop.jar -config resource/config.json -client -log resource/log_20221201.json -name execute -tail 10");
		System.out.println(
				" java -jar gop.jar -config resource/config.json -client -log resource/log_20221201.json -tag tag1 -head 10");
		System.out
				.println(" java -jar gop.jar -config resource/config.json -client -log resource/log_20221201.json");
		System.out.println(" ");
    }
}
