//
// Created by juan on 4/2/19.
//

#ifndef CASSANDRAAPPLICATIONS_PACKET_MANAGER_H
#define CASSANDRAAPPLICATIONS_PACKET_MANAGER_H

#include <cstdint>
#include <rte_ip.h>
#include <rte_ether.h>
#include <rte_tcp.h>
#include <string>
//We will only be using tcp for our protocol, so protocol will not be in 5 tuple

struct ipv4_5tuple {
    uint32_t ip_src;
    uint32_t ip_dst;
    uint16_t port_src;
    uint16_t port_dst;
};

struct ip_port{
    uint32_t ip;
    uint16_t port;
    bool initialized;
};
ipv4_hdr * get_ip_header(struct rte_mbuf *mbuf){
    return rte_pktmbuf_mtod_offset(mbuf,
                            struct ipv4_hdr *, sizeof(struct ether_hdr));
}


tcp_hdr * get_tcp_header(ipv4_hdr *iph){

    return  (tcp_hdr * )((u_char *) iph +((iph->version_ihl & IPV4_HDR_IHL_MASK) << 2));
}

ipv4_5tuple get_5_tuple(ipv4_hdr *iph,tcp_hdr * tcph){
    ipv4_5tuple tuple5 = {
            iph->src_addr,
            iph->dst_addr,
            tcph->src_port,
            tcph->dst_port
    };
    return tuple5;

}

const char * create_5_tuple_key(ipv4_5tuple tuple5){
    std::string tuple5_key = std::to_string(tuple5.ip_src)+std::to_string(tuple5.ip_dst)+std::to_string(tuple5.port_src)+std::to_string(tuple5.port_dst);
    return tuple5_key.c_str();
}





#endif //CASSANDRAAPPLICATIONS_PACKET_MANAGER_H
