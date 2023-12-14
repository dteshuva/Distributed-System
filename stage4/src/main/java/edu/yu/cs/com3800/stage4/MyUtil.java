package edu.yu.cs.com3800.stage4;

import java.util.List;

public class MyUtil {
    public static boolean containsIgnoreCase(List<String> list, String target) {
        if (list == null) {
            return false;
        }
        for (String string : list) {
            if (string.equalsIgnoreCase(target)) {
                return true;
            }
        }
        return false;
    }
}
