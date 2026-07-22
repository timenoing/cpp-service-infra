#include"logger.h"
Logger& Logger::Instance()
    {
        static Logger instance;
        return instance;
    }
 std::string Logger::GetCurrentTime() /*获取时间*/
    {
    std::time_t gettime=time(nullptr);
     tm* t = localtime(&gettime);
    char buf[64]={0};
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", t);
    return std::string(buf);
    }
     std::string Logger::Format( LogLevel level,const std::string& msg,const std::string& operation)
     {
      std::string time_set=GetCurrentTime();
      std::string log_line;
      std::string level_str;
      switch(level)
      {
        case INFO:  level_str = "INFO";  break;
        case WARN:  level_str = "WARN";  break;
        case ERROR: level_str = "ERROR"; break;
        case FATAL: level_str = "FATAL"; break;
        default:    level_str = "UNKNOWN"; break;
      }
      log_line.reserve(time_set.size() + level_str.size() +msg.size() +operation.size() );
      log_line="[" + time_set + "][" + level_str + "]["+ msg +"][" + operation + "]";
      return log_line;
     }


      void Logger::WriteToFile(const std::string& log_line)
     {
      std::lock_guard<std::mutex> lock_(lock_mut);
      
      if(log_file_.is_open())
      {
      pool_.submit([this,log_line]{log_file_<<log_line<<std::endl;});
      }
      else if(old_file_.is_open())
      {
         if(!switched_to_backup_ )
      {
      std::cout << "主日志文件损坏，请检查！，现在启用备份日志" << std::endl;
         switched_to_backup_=true;
   
      }
      pool_.submit([this,log_line]{old_file_<<log_line<<std::endl;});
        
        
      }
      else {
         if(!both_dead_warned_)
         {
         std::cout << "主日志文件和备份日志均损坏，请检查！" << std::endl;
        
         both_dead_warned_ =true;
      
         
         }
      }
   
     }


    void Logger::Log(LogLevel level, const std::string& msg,const std::string& opreation)
     {
          std::string log_line=Format(level, msg, opreation);
          WriteToFile(log_line);
     }


    Logger::Logger()
    : pool_(1)
     {
      log_file_.open("./logs/app.log",std::ios::app);
      old_file_.open("./logs/old_app.log",std::ios::app);
      
     }
     Logger::~Logger()
     {
      if(log_file_.is_open())
         log_file_.close();
      if(old_file_.is_open())
        old_file_.close();
     }