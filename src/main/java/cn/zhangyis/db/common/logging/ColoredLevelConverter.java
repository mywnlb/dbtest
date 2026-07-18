package cn.zhangyis.db.common.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Logback 级别颜色转换器。只改变控制台级别文本颜色，不改变日志内容和业务语义。
 */
public final class ColoredLevelConverter extends ClassicConverter {

    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据库通用基础设施的不变量。
     */
    private static final String RESET = "\u001B[0m";
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据库通用基础设施的不变量。
     */
    private static final String RED = "\u001B[31m";
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据库通用基础设施的不变量。
     */
    private static final String YELLOW = "\u001B[33m";

    /**
     * 根据日志级别返回带 ANSI 颜色的级别文本；ERROR 为红色、WARN 为黄色，其余级别保持原样。
     *
     * @param event Logback 日志事件，提供当前日志级别和格式化上下文。
     * @return 控制台输出使用的级别文本。
     */
    @Override
    public String convert(ILoggingEvent event) {
        String level = event.getLevel().toString();
        if (event.getLevel() == Level.ERROR) {
            return RED + level + RESET;
        }
        if (event.getLevel() == Level.WARN) {
            return YELLOW + level + RESET;
        }
        return level;
    }
}
