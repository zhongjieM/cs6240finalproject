package edu.neu.cs6240.project.preprocess;

public class PreProcessUtils {

	public static boolean isPageName(String pageName) {
		// Ignore all other cases to speed up.
		for (char c : pageName.toCharArray())
			if (!(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0'
					&& c <= '9' || c == '_'))
				return false;
		return true;
	}

	public static String[] splitPageName(String pageName) {
		return pageName.split("_");
	}

}
