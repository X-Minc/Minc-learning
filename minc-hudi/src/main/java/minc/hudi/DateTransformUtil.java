package minc.hudi;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 处理时间的工具类
 *
 * @Author: Minc
 * @DateTime: 2022.5.18 0018
 */
public class DateTransformUtil {

    /**
     * 获取日期偏移量后的字符串日期
     *
     * @param timestamp 时间戳
     * @param format 结果字符串的格式
     * @param offset 偏移量
     * @return 结果字符串
     */
    public static String getStringDateAfterOffsetFromDate(Long timestamp, String format, Duration offset) {
        LocalDateTime localDateTime;
        localDateTime = LocalDateTime.ofInstant(new Date(timestamp).toInstant(), ZoneId.systemDefault());
        localDateTime = localDateTime.plus(offset);
        return localDateTime.format(DateTimeFormatter.ofPattern(format));
    }

    /**
     * 获取字符串日期偏移后的日期
     *
     * @param date 字符串格式的日期
     * @param format 字符串格式
     * @param offset 偏移量
     * @return 时间戳
     */
    public static Long getDateAfterOffsetFromStringDate(String date, String format, Duration offset) {
        LocalDateTime localDateTime;
        localDateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(format));
        localDateTime = localDateTime.plus(offset);
        return localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }

    /**
     * 获取从字符串日期偏移量后天的零时间
     *
     * @param date 字符串格式的日期
     * @param format 字符串格式
     * @param offset 偏移量
     * @return 时间戳
     */
    public static Long getZeroTimeOfDayAfterFromStringDateOffset(String date, String format, Duration offset) {
        LocalDateTime localDateTime;
        localDateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(format));
        localDateTime = getZeroDateTime(offset, localDateTime);
        return localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }

    /**
     * 获取日期偏移量后的字符串日期
     *
     * @param timestamp 时间戳
     * @param format 结果字符串的格式
     * @param offset 偏移量
     * @return 结果字符串
     */
    public static String getZeroTimeOfDayAfterFromDateOffset(Long timestamp, String format, Duration offset) throws Exception {
        LocalDateTime localDateTime;
        localDateTime = LocalDateTime.ofInstant(new Date(timestamp).toInstant(), ZoneId.systemDefault());
        localDateTime = getZeroDateTime(offset, localDateTime);
        return localDateTime.format(DateTimeFormatter.ofPattern(format));
    }

    private static LocalDateTime getZeroDateTime(Duration offset, LocalDateTime localDateTime) {
        localDateTime = localDateTime.plusHours(localDateTime.getHour() * -1);
        localDateTime = localDateTime.plusMinutes(localDateTime.getMinute() * -1);
        localDateTime = localDateTime.plusSeconds(localDateTime.getSecond() * -1);
        localDateTime = localDateTime.plusNanos(localDateTime.getNano() * -1);
        localDateTime = localDateTime.plus(offset);
        return localDateTime;
    }


    public static Long getWindowStart(long now, long winSize) throws Exception {
        return TimeWindow.getWindowStartWithOffset(now, 0, winSize);
    }
}
