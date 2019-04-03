#include <iostream>
#include <rte_ip.h>
#include <rte_tcp.h>
#include <assert.h>
#include <cassandra.h>
#include "packet_manager.h"
#include "cassandra_manager.h"


CassFuture* connect_future;
CassCluster* cluster;
CassSession* session;
CassFuture* close_future;

std::string delimiter = ";";

int init() {
    connect_future = NULL;
    cluster = create_cluster();
    session = cass_session_new();;
    if (connect_session(session, cluster) != CASS_OK) {
        cass_cluster_free(cluster);
        cass_session_free(session);
        std::cout<<"Unable to connect"<<std::endl;
        return -1;
    }
    std::cout<<"Connected succesfully"<<std::endl;
    return 0;
}

const char * create_ip_port_key(ip_port ipPort){
    std::string ip_port_key = std::to_string(ipPort.ip)+delimiter+std::to_string(ipPort.port);
    return ip_port_key.c_str();
}

ip_port select_from_used_ports(const char * tuple5Key){
    const char* query = "SELECT ip,port FROM usedports WHERE key=?";
    CassStatement* statement = cass_statement_new(query, 1);
    cass_statement_bind_string(statement, 0, tuple5Key);
    ip_port ipPort;
    int32_t ip = -1;
    int16_t port =-1;

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
        /* Retrieve result set and get the first row */

        const CassResult* result = cass_future_get_result(result_future);
        const CassRow* row = cass_result_first_row(result);

        if (row) {
            const CassValue* ip_value = cass_row_get_column_by_name(row, "ip");
            cass_value_get_int32(ip_value, &ip);
            std::cout<<"Ip :"<< ip<<std::endl;
            const CassValue* port_value = cass_row_get_column_by_name(row, "port");
            cass_value_get_int16(port_value, &port);
            std::cout<<"Port :"<< port<<std::endl;
        }

        cass_result_free(result);
    } else {
        /* Handle erroused_portr */
        const char* message;
        size_t message_length;
        cass_future_error_message(result_future, &message, &message_length);
        fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
    }
    if(ip==-1||port==-1){
        ipPort.initialized = false;
    }
    else{
        ipPort.initialized = true;
        ipPort.ip = ip;
        ipPort.port = port;
    }
    return ipPort;
}

ip_port select_ip_port() {
    CassError rc = CASS_OK;
    CassStatement* statement = NULL;
    CassFuture* future = NULL;
    const char* query = "SELECT * FROM ports";
    ip_port ipPort;
    const char * ip_port_char =NULL;
    size_t size;

    statement = cass_statement_new(query, 0);
    future = cass_session_execute(session, statement);
    cass_future_wait(future);

    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        print_error(future);
    } else {
        const CassResult* result = cass_future_get_result(future);
        CassIterator* iterator = cass_iterator_from_result(result);

        while (cass_iterator_next(iterator)) {
            cass_bool_t used;
            const CassRow* row = cass_iterator_get_row(iterator);

            cass_value_get_bool(cass_row_get_column_by_name(row, "used"), &used);
            if(!used){
                cass_value_get_string(cass_row_get_column_by_name(row, "key"),&ip_port_char,&size);
                break;
            }
        }
        cass_result_free(result);
        cass_iterator_free(iterator);
    }
    if(ip_port_char==NULL){
        std::cout<<"No unused port"<<std::endl;
        assert(false);
    }

    std::string ip_port_string(ip_port_char);
    int delimiter_pos = ip_port_string.find(delimiter);
    ipPort.ip  = std::stoi(ip_port_string.substr(0,delimiter_pos ));
    ipPort.port = std::stoi(ip_port_string.substr(delimiter_pos,size-delimiter_pos));
    cass_future_free(future);
    cass_statement_free(statement);
    return ipPort;
}
void update_ip_ports_db(ip_port ipPort, cass_bool_t used){
    CassError rc = CASS_OK;
    CassStatement* statement = NULL;
    CassFuture* future = NULL;
    const char * ip_port_key = create_ip_port_key(ipPort);
    const char* insert = "INSERT INTO ports (key,used) values (?, ?)";

    statement = cass_statement_new(insert, 2);

    cass_statement_bind_string(statement, 0, ip_port_key);
    cass_statement_bind_bool(statement, 1,used);
    future = cass_session_execute(session, statement);
    cass_future_wait(future);

    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        print_error(future);
        assert(false);
    }

    cass_future_free(future);
    cass_statement_free(statement);
}

void update_used_ports(const char * tuple5_key, ip_port ipPort){
    CassError rc = CASS_OK;
    CassStatement* statement = NULL;
    CassFuture* future = NULL;
    const char* insert = "Insert into usedports (key,ip,port)";

    statement = cass_statement_new(insert, 3);

    cass_statement_bind_string(statement, 0, tuple5_key);
    cass_statement_bind_int32(statement, 1, ipPort.ip);
    cass_statement_bind_int16(statement, 2,ipPort.port);
    future = cass_session_execute(session, statement);
    cass_future_wait(future);

    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        print_error(future);
        assert(false);
    }

    cass_future_free(future);
    cass_statement_free(statement);
}
ipv4_5tuple extract_reverse_tuple(ipv4_5tuple tuple5, ip_port ipPort){
    ipv4_5tuple reverse_tuple = {
            ipPort.ip,
            tuple5.ip_src,
            ipPort.port,
            tuple5.port_src
    };
    return reverse_tuple;
}

static int packet_processing(struct rte_mbuf *mbuf) {
    ipv4_hdr * iph = get_ip_header(mbuf);
    if (iph->next_proto_id != IPPROTO_TCP)
        return 0;

    tcp_hdr *tcph = get_tcp_header(iph);
    ipv4_5tuple tuple5 = get_5_tuple(iph,tcph);
    ip_port dest;
    const char * tuple5_key =  create_5_tuple_key(tuple5);
    ip_port used_ip_port = select_from_used_ports(tuple5_key);
    if(!used_ip_port.initialized){
        dest = select_ip_port();
        update_ip_ports_db(dest,cass_true);
        update_used_ports(tuple5_key,dest);
        ipv4_5tuple reverse_tuple = extract_reverse_tuple(tuple5,dest);
        ip_port src_ip_port = {
                tuple5.ip_src,
                tuple5.port_src
        };
        update_used_ports(create_5_tuple_key(reverse_tuple),src_ip_port);
    }
    else{
        dest = used_ip_port;
    }
    iph->dst_addr =dest.ip;
    tcph->dst_port = dest.port;
    return  1;
}


int main() {
    std::cout << "Hello, World!" << std::endl;
    return 0;
}