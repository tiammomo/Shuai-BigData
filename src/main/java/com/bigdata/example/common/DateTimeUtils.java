package com.bigdata.example.common;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.Date;

/**
 * 日期时间工具类 (Java 8+)
 */
public class DateTimeUtils {

    private static final DateTimeFormatter DEFAULT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");

    // ==================== 格式转换 ====================

    /**
     * LocalDateTime转字符串
     */
    public static String format(LocalDateTime dateTime) {
        return dateTime != null ? dateTime.format(DEFAULT_FORMAT) : null;
    }

    public static String format(LocalDateTime dateTime, String pattern) {
        return dateTime != null ? dateTime.format(DateTimeFormatter.ofPattern(pattern)) : null;
    }

    /**
     * 字符串转LocalDateTime
     */
    public static LocalDateTime parseDateTime(String dateTimeStr) {
        return LocalDateTime.parse(dateTimeStr, DEFAULT_FORMAT);
    }

    public static LocalDateTime parseDateTime(String dateTimeStr, String pattern) {
        return LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern(pattern));
    }

    /**
     * 字符串转LocalDate
     */
    public static LocalDate parseDate(String dateStr) {
        return LocalDate.parse(dateStr, DATE_FORMAT);
    }

    /**
     * Date转LocalDateTime
     */
    public static LocalDateTime toLocalDateTime(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    /**
     * LocalDateTime转Date
     */
    public static Date toDate(LocalDateTime dateTime) {
        return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    // ==================== 日期计算 ====================

    /**
     * 获取当前时间的毫秒戳
     */
    public static long nowMillis() {
        return System.currentTimeMillis();
    }

    /**
     * 获取当前时间的秒戳
     */
    public static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * 日期加减天数
     */
    public static LocalDate addDays(LocalDate date, int days) {
        return date.plusDays(days);
    }

    public static LocalDate subtractDays(LocalDate date, int days) {
        return date.minusDays(days);
    }

    /**
     * 日期加减月份
     */
    public static LocalDate addMonths(LocalDate date, int months) {
        return date.plusMonths(months);
    }

    public static LocalDate subtractMonths(LocalDate date, int months) {
        return date.minusMonths(months);
    }

    /**
     * 时间加减小时
     */
    public static LocalDateTime addHours(LocalDateTime dateTime, int hours) {
        return dateTime.plusHours(hours);
    }

    public static LocalDateTime subtractHours(LocalDateTime dateTime, int hours) {
        return dateTime.minusHours(hours);
    }

    // ==================== 日期提取 ====================

    /**
     * 获取日期的年份
     */
    public static int getYear(LocalDate date) {
        return date.getYear();
    }

    /**
     * 获取日期的月份
     */
    public static int getMonth(LocalDate date) {
        return date.getMonthValue();
    }

    /**
     * 获取日期的日
     */
    public static int getDay(LocalDate date) {
        return date.getDayOfMonth();
    }

    /**
     * 获取星期几 (1=Monday, 7=Sunday)
     */
    public static int getDayOfWeek(LocalDate date) {
        return date.getDayOfWeek().getValue();
    }

    /**
     * 获取季度
     */
    public static int getQuarter(LocalDate date) {
        return (date.getMonthValue() - 1) / 3 + 1;
    }

    // ==================== 日期边界 ====================

    /**
     * 获取月份第一天
     */
    public static LocalDate firstDayOfMonth(LocalDate date) {
        return date.with(TemporalAdjusters.firstDayOfMonth());
    }

    /**
     * 获取月份最后一天
     */
    public static LocalDate lastDayOfMonth(LocalDate date) {
        return date.with(TemporalAdjusters.lastDayOfMonth());
    }

    /**
     * 获取周一开始的日期
     */
    public static LocalDate mondayOfWeek(LocalDate date) {
        return date.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
    }

    /**
     * 获取周日开始(美国习惯)
     */
    public static LocalDate sundayOfWeek(LocalDate date) {
        return date.with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY));
    }

    /**
     * 获取季度第一天
     */
    public static LocalDate firstDayOfQuarter(LocalDate date) {
        int quarter = getQuarter(date);
        int firstMonth = (quarter - 1) * 3 + 1;
        return LocalDate.of(date.getYear(), firstMonth, 1);
    }

    /**
     * 获取季度最后一天
     */
    public static LocalDate lastDayOfQuarter(LocalDate date) {
        int quarter = getQuarter(date);
        int lastMonth = quarter * 3;
        return LocalDate.of(date.getYear(), lastMonth, 1).with(TemporalAdjusters.lastDayOfMonth());
    }

    /**
     * 获取年份第一天
     */
    public static LocalDate firstDayOfYear(LocalDate date) {
        return date.with(TemporalAdjusters.firstDayOfYear());
    }

    /**
     * 获取年份最后一天
     */
    public static LocalDate lastDayOfYear(LocalDate date) {
        return date.with(TemporalAdjusters.lastDayOfYear());
    }

    // ==================== 时区转换 ====================

    /**
     * 转换时区
     */
    public static LocalDateTime convertZone(LocalDateTime dateTime, ZoneId fromZone, ZoneId toZone) {
        ZonedDateTime zonedDateTime = dateTime.atZone(fromZone);
        return zonedDateTime.withZoneSameInstant(toZone).toLocalDateTime();
    }

    /**
     * UTC转本地时间
     */
    public static LocalDateTime utcToLocal(LocalDateTime utcDateTime) {
        return convertZone(utcDateTime, ZoneOffset.UTC, ZoneId.systemDefault());
    }

    /**
     * 本地时间转UTC
     */
    public static LocalDateTime localToUtc(LocalDateTime localDateTime) {
        return convertZone(localDateTime, ZoneId.systemDefault(), ZoneOffset.UTC);
    }

    // ==================== 时间差计算 ====================

    /**
     * 计算两个日期相差天数
     */
    public static long daysBetween(LocalDate start, LocalDate end) {
        return java.time.temporal.ChronoUnit.DAYS.between(start, end);
    }

    /**
     * 计算两个日期相差月份
     */
    public static long monthsBetween(LocalDate start, LocalDate end) {
        return java.time.temporal.ChronoUnit.MONTHS.between(start, end);
    }

    /**
     * 计算两个时间相差小时
     */
    public static long hoursBetween(LocalDateTime start, LocalDateTime end) {
        return java.time.temporal.ChronoUnit.HOURS.between(start, end);
    }

    /**
     * 计算两个时间相差分钟
     */
    public static long minutesBetween(LocalDateTime start, LocalDateTime end) {
        return java.time.temporal.ChronoUnit.MINUTES.between(start, end);
    }

    /**
     * 计算两个时间相差秒
     */
    public static long secondsBetween(LocalDateTime start, LocalDateTime end) {
        return java.time.temporal.ChronoUnit.SECONDS.between(start, end);
    }
}
