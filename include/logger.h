#ifndef LOGGER_H
#define LOGGER_H

#include <iostream>
#include <mutex>
#include <thread>
#include <string>
#include <ctime>
#include <fstream>
#include<functional>
#include"ThreadPool.h"
    enum LogLevel {
    INFO,
    WARN,
    ERROR,
    FATAL
};


class Logger {
public:

    static Logger& Instance();
    void Log(LogLevel level, const std::string& msg,const std::string& opreation);

private:
    ThreadPool pool_;
    Logger();   // 构造函数，打开文件
    ~Logger();  // 析构函数，关闭文件
    std::string GetCurrentTime();                            // 获取当前时间字符串
    std::string Format(LogLevel level, const std::string& msg,const std::string& opreation); // 拼装完整日志行
    void WriteToFile(const std::string& log_line);            // 输出到控制台和文件
    void Rotate();                                            // 日志文件回滚/归档
    std::ofstream log_file_;   // 主日志文件
    std::ofstream old_file_;   // 旧日志文件
    bool switched_to_backup_ =false;//备份启用
    bool both_dead_warned_ = false;//两份全部不能用
};

#endif