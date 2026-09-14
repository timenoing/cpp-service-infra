#include<Message.h>
std::string net::encode(const std::string &msg) //构造成四字节全是长度，编码器
{
   uint32_t len=htonl(static_cast<uint32_t>(msg.size()));
   std::string packet;
   packet.reserve(4+msg.size());
   packet.append(reinterpret_cast<char*>(&len), 4);
   packet.append(msg);
   return packet;
}
void net::messagedecode::feed(const char *data, ssize_t n) //messagedecode是一个解码器，feed是投给主线程的
{
  if(error!=false)
  return;
     Inputbuf.insert(Inputbuf.end(), data, data+n);
     while(1)
     {
      if(idx>1024*512)
      {
        std::vector<char> temp;
        temp.insert(temp.begin(), Inputbuf.begin()+idx, Inputbuf.end());
        Inputbuf.clear();
        Inputbuf=std::move(temp);
        idx=0;
      }
      if(Inputbuf.size()-idx<4)
      {
        return;
      }else{
        uint32_t len=0;
        std::memcpy(&len,Inputbuf.data()+idx,4);
        uint32_t msg_len=ntohl(len);
        if(msg_len>messagemaxlen)
        {
          error=true;
          Inputbuf.clear();
          idx=0; //错误，信息已经不可靠了直接全丢
          return;
        }else if(Inputbuf.size()-idx-4<msg_len) //半包返回等下一轮写
        return;
        else{
          std::string ans;
          idx+=4;
          ans.append(Inputbuf.begin()+idx, Inputbuf.begin()+idx+msg_len);
          msg.push_back(ans);
          idx+=msg_len;  //正常路径加，也是正常下，唯一改动idx的路径
        }
      }
     }

}
std::vector<std::string> net::messagedecode::take()//take拉消息
{
  std::vector<std::string> ans;
  ans=std::move(msg);
    return ans;
}
bool net::messagedecode::brokenmessage()//判断消息是否有坏帧
{
  return error;
}