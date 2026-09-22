#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <Message.h>

struct Conn{
    int fd=-1;
    int ev_cur=0;
    uint64_t next_id=0;
    uint64_t done_cnt=0;
    uint64_t inflight=0;
    uint64_t per_conn=0;
    int size=0;
    bool check=false;
    bool finished=false;
    std::string outbuf;
    net::messagedecode dec;
    std::unordered_map<uint64_t,std::chrono::steady_clock::time_point> pending;
};

static std::string make_msg(uint64_t id,int size){
    std::string m(size,'\0');
    std::memcpy(m.data(),&id,8);
    for(int i=8;i<size;++i)
        m[i]=(char)((id*2654435761ull+(uint64_t)i*40503ull)>>13);
    return m;
}

static bool check_msg(uint64_t id,const std::string& m,int size){
    if((int)m.size()!=size) return false;
    if(m.compare(8,0,"")!=0) return false;
    for(int i=8;i<size;++i){
        char want=(char)((id*2654435761ull+(uint64_t)i*40503ull)>>13);
        if(m[i]!=want) return false;
    }
    return true;
}

static bool set_nonblocking(int fd){
    int fl=fcntl(fd,F_GETFL,0);
    if(fl<0) return false;
    return fcntl(fd,F_SETFL,fl|O_NONBLOCK)==0;
}

static void set_events(int ep,Conn& c,int want){
    if(want==c.ev_cur) return;
    epoll_event ev{};
    ev.events=want;
    ev.data.fd=c.fd;
    if(c.ev_cur==0)
        epoll_ctl(ep,EPOLL_CTL_ADD,c.fd,&ev);
    else
        epoll_ctl(ep,EPOLL_CTL_MOD,c.fd,&ev);
    c.ev_cur=want;
}

static bool flush_out(Conn& c){
    while(!c.outbuf.empty()){
        ssize_t n=send(c.fd,c.outbuf.data(),c.outbuf.size(),MSG_NOSIGNAL);
        if(n>0){
            c.outbuf.erase(0,(size_t)n);
            continue;
        }
        if(n<0&&errno==EINTR) continue;
        if(n<0&&(errno==EAGAIN||errno==EWOULDBLOCK)) return true;
        return false;
    }
    return true;
}

static bool pump_send(int ep,Conn& c,uint64_t expected_inflight){
    while(c.next_id<c.per_conn&&c.inflight<expected_inflight){
        uint64_t id=c.next_id++;
        std::string packet=net::encode(make_msg(id,c.size));
        c.pending[id]=std::chrono::steady_clock::now();
        c.outbuf.append(packet);
        c.inflight++;
    }
    if(!flush_out(c)) return false;
    int want=EPOLLIN;
    if(!c.outbuf.empty()) want|=EPOLLOUT;
    set_events(ep,c,want);
    return true;
}

int main(int argc,char** argv){
    int conns=1,per_conn=100,size=64,depth=1,port=8888,deadline_s=120;
    bool check=false;
    for(int i=1;i<argc;++i){
        std::string a=argv[i];
        auto val=[&]()->int{
            if(i+1>=argc){fprintf(stderr,"missing value for %s\n",a.c_str());exit(2);}
            return std::atoi(argv[++i]);
        };
        if(a=="-c") conns=val();
        else if(a=="-n") per_conn=val();
        else if(a=="-s") size=val();
        else if(a=="-d") depth=val();
        else if(a=="-p") port=val();
        else if(a=="-t") deadline_s=val();
        else if(a=="--check") check=true;
        else{
            fprintf(stderr,"usage: bench_client [-c conns] [-n msgs_per_conn] [-s size] [-d depth] [-p port] [-t sec] [--check]\n");
            return 2;
        }
    }
    if(conns<=0||per_conn<=0||depth<=0||deadline_s<=0){
        fprintf(stderr,"bad args\n");
        return 2;
    }
    if(size<8){
        fprintf(stderr,"size %d < 8, clamped to 8 (8B id overhead)\n",size);
        size=8;
    }
    if(size>65535){
        fprintf(stderr,"size %d > 65535 protocol limit\n",size);
        return 2;
    }

    int ep=epoll_create1(0);
    if(ep<0){
        perror("epoll_create1");
        return 1;
    }
    std::unordered_map<int,Conn> cs;
    cs.reserve((size_t)conns);
    for(int i=0;i<conns;++i){
        int fd=socket(AF_INET,SOCK_STREAM,0);
        if(fd<0){
            perror("socket");
            return 1;
        }
        sockaddr_in addr{};
        addr.sin_family=AF_INET;
        addr.sin_port=htons((uint16_t)port);
        inet_pton(AF_INET,"127.0.0.1",&addr.sin_addr);
        if(connect(fd,(sockaddr*)&addr,sizeof(addr))<0){
            perror("connect");
            close(fd);
            return 1;
        }
        set_nonblocking(fd);
        int one=1;
        setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
        Conn c{};
        c.fd=fd;
        c.per_conn=(uint64_t)per_conn;
        c.size=size;
        c.check=check;
        cs.emplace(fd,std::move(c));
        int want=EPOLLIN;
        set_events(ep,cs.at(fd),want);
        if(!pump_send(ep,cs.at(fd),(uint64_t)depth)){
            fprintf(stderr,"initial send failed on conn %d\n",i);
            return 1;
        }
    }

    uint64_t expected=(uint64_t)conns*(uint64_t)per_conn;
    uint64_t received=0,extra=0,errors=0,tampered=0;
    std::vector<long long> lat_us;
    lat_us.reserve(expected);

    auto t0=std::chrono::steady_clock::now();
    auto deadline=t0+std::chrono::seconds(deadline_s);
    std::vector<epoll_event> evs(256);
    size_t alive=cs.size();

    while(alive>0){
        if(std::chrono::steady_clock::now()>deadline){
            fprintf(stderr,"DEADLINE: %.1fs elapsed, aborting\n",deadline_s*1.0);
            break;
        }
        int nfds=epoll_wait(ep,evs.data(),(int)evs.size(),1000);
        if(nfds<0){
            if(errno==EINTR) continue;
            perror("epoll_wait");
            break;
        }
        for(int k=0;k<nfds;++k){
            int fd=evs[k].data.fd;
            auto it=cs.find(fd);
            if(it==cs.end()) continue;
            Conn& c=it->second;
            uint32_t e=evs[k].events;
            bool fail=false;
            if(e&(EPOLLERR|EPOLLHUP)){
                fail=true;
            }
            if(!fail&&(e&EPOLLOUT)){
                if(!flush_out(c)) fail=true;
                else{
                    int want=EPOLLIN;
                    if(!c.outbuf.empty()) want|=EPOLLOUT;
                    set_events(ep,c,want);
                }
            }
            if(!fail&&(e&EPOLLIN)){
                char buf[65536];
                while(true){
                    ssize_t n=recv(fd,buf,sizeof(buf),0);
                    if(n>0){
                        c.dec.feed(buf,n);
                        if(c.dec.brokenmessage()){
                            fail=true;
                            break;
                        }
                        std::vector<std::string> got=c.dec.take();
                        auto now=std::chrono::steady_clock::now();
                        for(size_t m=0;m<got.size();++m){
                            const std::string& msg=got[m];
                            if(msg.size()<8){
                                extra++;
                                continue;
                            }
                            uint64_t id=0;
                            std::memcpy(&id,msg.data(),8);
                            auto pit=c.pending.find(id);
                            if(pit==c.pending.end()){
                                extra++;
                                continue;
                            }
                            lat_us.push_back(std::chrono::duration_cast<std::chrono::microseconds>(now-pit->second).count());
                            c.pending.erase(pit);
                            c.inflight--;
                            c.done_cnt++;
                            received++;
                            if(check&&!check_msg(id,msg,c.size)) tampered++;
                        }
                        if(c.done_cnt>=c.per_conn) break;
                        continue;
                    }
                    if(n==0){
                        if(c.done_cnt<c.per_conn) fail=true;
                        break;
                    }
                    if(errno==EINTR) continue;
                    if(errno==EAGAIN||errno==EWOULDBLOCK) break;
                    fail=true;
                    break;
                }
            }
            if(!fail&&!c.finished&&c.done_cnt<c.per_conn){
                if(!pump_send(ep,c,(uint64_t)depth)) fail=true;
            }
            if(fail){
                errors++;
                epoll_ctl(ep,EPOLL_CTL_DEL,fd,nullptr);
                close(fd);
                cs.erase(it);
                alive--;
                continue;
            }
            if(c.done_cnt>=c.per_conn&&c.inflight==0&&c.outbuf.empty()){
                c.finished=true;
                epoll_ctl(ep,EPOLL_CTL_DEL,fd,nullptr);
                close(fd);
                cs.erase(it);
                alive--;
            }
        }
    }
    double elapsed=std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now()-t0).count();
    uint64_t lost=expected>received?expected-received:0;
    double qps=elapsed>0?(double)received/elapsed:0;

    std::sort(lat_us.begin(),lat_us.end());
    auto pct=[&](double q)->double{
        if(lat_us.empty()) return 0;
        size_t idx=(size_t)(q*(double)(lat_us.size()-1));
        return (double)lat_us[idx]/1000.0;
    };

    printf("conns=%d msgs=%llu size=%dB depth=%d time=%.3fs\n",
           conns,(unsigned long long)expected,size,depth,elapsed);
    printf("qps=%.0f p50=%.3fms p90=%.3fms p99=%.3fms p999=%.3fms max=%.3fms\n",
           qps,pct(0.50),pct(0.90),pct(0.99),pct(0.999),
           lat_us.empty()?0.0:(double)lat_us.back()/1000.0);
    printf("recv=%llu lost=%llu extra=%llu errors=%llu tampered=%llu\n",
           (unsigned long long)received,(unsigned long long)lost,
           (unsigned long long)extra,(unsigned long long)errors,(unsigned long long)tampered);

    close(ep);
    bool ok=(received==expected&&extra==0&&errors==0&&tampered==0&&alive==0);
    printf("%s\n",ok?"PASS":"FAIL");
    return ok?0:1;
}
