#include"Connection.h"
#include<iostream>
Connection::Connection(const std::string& filename)
{
  int r=sqlite3_open(filename.c_str(),&db_);
  
  if(r!=0)
  {
  std::cerr<<"出现错误"<<sqlite3_errstr(r)<<std::endl;
  if(db_)
  {sqlite3_close(db_);db_=nullptr;  }  return ;
  }
  else
  {
  sqlite3_busy_timeout(db_, 3000);
  sqlite3_exec(db_, "PRAGMA journal_mode=WAL;",nullptr, nullptr, nullptr);
  }

}
Connection::~Connection()
{
  if(db_)
  sqlite3_close(db_);
}
Cursor* Connection::query(const std::string& sql)
{
if (!db_) return nullptr;
sqlite3_stmt* stmt=nullptr;
int rc =sqlite3_prepare_v2(db_,sql.c_str(),-1,&stmt,nullptr);
if(rc==SQLITE_OK)
return new Cursor(stmt);
else {std::cerr<<"错误"<<sqlite3_errmsg(db_)<<std::endl; return nullptr;}
}
bool Connection::execute(const std::string& sql)
{
  if (!db_) return false;
  char *errmsg=nullptr;
  sqlite3_exec(db_,sql.c_str(),nullptr,nullptr,&errmsg);
  if(errmsg)
  {
    std::cerr<<errmsg<<std::endl;
    sqlite3_free(errmsg);
    return false;
  }
  return true;
}
bool Connection::ping()
{
  if(!db_) return false;
  return execute("SELECT 1;");
}
bool Connection::isValid()
{
    return db_!=nullptr;
}