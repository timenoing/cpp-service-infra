#include"ConnectionGuard.h"
ConnectionGuard::ConnectionGuard(Connection* conn,std::function<void(Connection*)>cb)
:conn_(conn),return_callback_(std::move(cb))
{
}
ConnectionGuard::~ConnectionGuard()
{
    if(conn_&&return_callback_)
    {
        return_callback_(conn_);
    }
}
ConnectionGuard::ConnectionGuard(ConnectionGuard&& other) noexcept
:conn_(other.conn_),return_callback_(std::move(other.return_callback_))
{
    other.conn_=nullptr;
    other.return_callback_=nullptr;
}
ConnectionGuard& ConnectionGuard::operator=(ConnectionGuard&& other) noexcept
{
    if(this!=&other)
    {
      if(conn_&&return_callback_)
      {
        return_callback_(conn_);
      }
      conn_=other.conn_;
      return_callback_=std::move(other.return_callback_);
      other.conn_=nullptr;
      other.return_callback_=nullptr;

    }
    return *this;
}
Connection* ConnectionGuard::operator->()
{
    return conn_;
}
