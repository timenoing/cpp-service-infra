#ifndef CURSOR_H
#define CURSOR_H
#include<sqlite3.h>
#include<string>
#include<iostream>
#include<utility>
class Cursor
{
public:
  explicit Cursor(sqlite3_stmt* stmt);
  ~Cursor();
  Cursor(const Cursor&)=delete;
  Cursor& operator=(const Cursor&)=delete;
  Cursor( Cursor&&) noexcept;
  Cursor& operator=( Cursor&&) noexcept;
  bool next();
  int getInt(int col);
  std::string getString(int col);
  int columnCount();
private:
sqlite3_stmt* stmt_;
};


#endif