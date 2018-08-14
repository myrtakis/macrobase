package alexp.macrobase.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimeUtils {
    public static long dateTimeToUnixTimestamp(String dt, String format) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

        dt = dt.replace(".000000", "");

        LocalDateTime localDateTime = LocalDateTime.parse(dt, formatter);

        return localDateTime.atZone(ZoneId.of("UTC")).toInstant().getEpochSecond();
    }
}
