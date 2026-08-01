#ifndef CONNECTION_H
#define CONNECTION_H
#include<sqlite3.h>
#include<string>
#include"Cursor.h"
class Cursor;
class Connection
{
    public:
    Connection(const Connection&)=delete;
    Connection& operator=(const Connection&)=delete;
    Connection(Connection&&) = delete;
    Connection& operator=(Connection&&) = delete;
    bool execute(const std::string& sql);
    Cursor* query(const std::string& sql);
    Connection(const std::string& filename);
    ~Connection();
    bool ping();
    bool isvalid();
    private:
    sqlite3* db_;
  
};


#endif