package com.unime.dataSpace;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateExtractor {
    public static String[] extractDateRange(String input) {
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(input);
        if (!groups.isEmpty()) {
            List<Date> dates = groups.get(0).getDates();
            if (!dates.isEmpty()) {
                Calendar cal = Calendar.getInstance();
                cal.setTime(dates.get(0));
                int startYear = cal.get(Calendar.YEAR);

                // Fix: if year < 2014, force 2014 (you can adjust logic here)
                if (startYear > 2014) {
                    cal.set(Calendar.YEAR, 2014);
                    dates.set(0, cal.getTime());
                }

                String start = new SimpleDateFormat("yyyy-MM-dd").format(dates.get(0));
                String end = (dates.size() > 1)
                        ? new SimpleDateFormat("yyyy-MM-dd").format(dates.get(1))
                        : start;

                return new String[]{start, end};
            }
        }
        return null;
    }
}

