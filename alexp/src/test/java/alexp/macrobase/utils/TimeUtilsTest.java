package alexp.macrobase.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class TimeUtilsTest {

    @Test
    public void dateTimeToUnixTimestamp() {
        assertEquals(1397504700, TimeUtils.dateTimeToUnixTimestamp("2014-04-14 19:45:00", "yyyy-MM-dd HH:mm:ss"));
    }
}