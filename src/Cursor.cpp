#include"Cursor.h"
Cursor::Cursor(sqlite3_stmt* stmt)
  :stmt_(stmt)
{
}
Cursor::~Cursor()
{
    if(stmt_)
    sqlite3_finalize(stmt_);
}
Cursor::Cursor(Cursor&& other) noexcept :stmt_(other.stmt_)
{
    other.stmt_=nullptr;
}
Cursor& Cursor::operator=(Cursor&& other) noexcept
{
  if(this!=&other)
  {
    sqlite3_finalize(stmt_);
    stmt_=other.stmt_;
    other.stmt_=nullptr;
  }
  return *this;
}
bool Cursor::next()
{
  int rc=sqlite3_step(stmt_);
  if(rc==SQLITE_ROW)
  return true;
  else if(rc==SQLITE_DONE)
  return false;
  else {std::cerr<<"step错误"<<rc<<std::endl;  return false; }
}
int Cursor::getInt(int col)
{
  return  sqlite3_column_int(stmt_,col);
}
std::string Cursor::getString(int col)
{
   const unsigned char* text=sqlite3_column_text(stmt_,col);
   return text ? std::string (reinterpret_cast<const char*>(text)):std::string();

}
int Cursor::columnCount()
{
  return sqlite3_column_count(stmt_);
}