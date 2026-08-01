#ifndef CONNECTION_GUARD_H
#define CONNECTION_GUARD_H
#include<functional>
#include"Connection.h"
class ConnectionGuard
{
    public:
    ConnectionGuard(Connection*,std::function<void(Connection*)>);
    ~ConnectionGuard();

    ConnectionGuard (const ConnectionGuard&)=delete;
    ConnectionGuard& operator =(const ConnectionGuard&)=delete;
    
    ConnectionGuard (ConnectionGuard&& other) noexcept;
    ConnectionGuard& operator =(ConnectionGuard&& other) noexcept;
    Connection* operator->();
    bool empty();
    
    
    private:
    Connection* conn_;
    std::function<void(Connection* )> return_callback_;

};


#endif