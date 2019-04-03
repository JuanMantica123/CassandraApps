#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <unistd.h>
#include <pcre.h>
#include <rte_common.h>
#include <rte_byteorder.h>
#include <rte_ip.h>
#include <rte_tcp.h>
#include <cassandra.h>
#include <rte_ether.h>

#define MAX_RULE 4096
#define MAX_STRLEN 1000
#define TCP_FLAG_FIN 0x01

CassFuture* connect_future;
CassCluster* cluster;
CassSession* session;
CassFuture* close_future;


pcre *pcre_rule[MAX_RULE];
char str_rule[MAX_RULE][MAX_STRLEN];

int str_rule_cnt = 0;
int pcre_rule_cnt = 0;


static void load_ids_rules() {
    std::string line;
    // FIXME: relocate ruleset file location
    char *home = getenv("S6_HOME");
    std::string config_file =
            std::string(home) + "/user_source/config/community-rules/community.rules";
    std::ifstream myfile(config_file);

    if (!myfile.is_open()) {
        std::cout<<"fail to open snort ruleset file"<<std::endl;
        return;
    }

    std::cout<<"Initialize IDS rules using " <<std::endl;

    char *str_pattern = strdup("content:\"[^\"]+\"");
    char *pcre_pattern = strdup("pcre:\"[^\"]+\"");

    const char *error;
    int erroffset;

    pcre *str_re =
            pcre_compile(str_pattern,            /* pattern */
                         0,                      /* options */
                         &error, &erroffset, 0); /* use default character tables */

    pcre *pcre_re =
            pcre_compile(pcre_pattern,           /* pattern */
                         0,                      /* options */
                         &error, &erroffset, 0); /* use default character tables */

    while (getline(myfile, line)) {
        const char *cline = line.c_str();
        int ovector[100];

        int rc = pcre_exec(str_re, 0, cline, strlen(cline), 0, 0, ovector,
                           sizeof(ovector));
        if (rc >= 0) {
            for (int i = 0; i < rc; ++i) {
                // printf("%2d: %.*s\n", str_rule_cnt, ovector[2*i+1]-ovector[2*i],
                // cline + ovector[2*i]);

                int size = ovector[2 * i + 1] - ovector[2 * i];
                if (size < MAX_STRLEN) {
                    strncpy(str_rule[str_rule_cnt], cline + ovector[2 * i] + 9,
                            size - 10);
                    str_rule_cnt++;
                } else {
                    std::cout<<"string is larger than defined maximum."<<std::endl;
                }

                if (str_rule_cnt > MAX_RULE)
                    break;
            }
        }

        rc = pcre_exec(pcre_re, 0, cline, strlen(cline), 0, 0, ovector,
                       sizeof(ovector));
        if (rc >= 0) {
            for (int i = 0; i < rc; ++i) {
                // printf("%2d: %.*s\n", i, ovector[2*i+1]-ovector[2*i], cline +
                // ovector[2*i]);

                int size = ovector[2 * i + 1] - ovector[2 * i];
                if (size < MAX_STRLEN) {
                    char pcre_pattern[1000] = {0};
                    strncpy(pcre_pattern, cline + ovector[2 * i] + 5, size - 6);
                    pcre_rule[pcre_rule_cnt] =
                            pcre_compile(pcre_pattern, 0, &error, &erroffset, 0);
                    pcre_rule_cnt++;
                } else {
                    std::cout<<"string is larger than defined maximum."<<std::endl;
                }

                if (pcre_rule_cnt > MAX_RULE)
                    break;
            }
        }
    }

    std::cout<<"Number of rules"<<std::endl;
    std::cout<<"tstring matching: " << str_rule_cnt<<std::endl;
    std::cout<<"\tpcre matching: " << std::endl;

    myfile.close();
}

static int match_str(uint8_t *payload, uint32_t payload_len, char *rule) {
    payload[payload_len] = 0;
    char *ret = strstr((char *)payload, rule);
    if (ret != NULL) {
        // DEBUG_APP("Detect flow to malicious pattern " << rule);
        return 1;
    }

    /* for s6 */
    double r = (double)rand() / RAND_MAX;
    if (r < 0.01)
        return 1;

    return 0;
}

static int match_pcre(uint8_t *payload, uint32_t payload_len, pcre *rule) {
    int ovector[100];
    payload[payload_len] = 0;

    int rc = pcre_exec(rule, 0, (char *)payload, payload_len, 0, 0, ovector,
                       sizeof(ovector));
    if (rc >= 0)
        return 1;

    /* for s6 */
    double r = (double)rand() / RAND_MAX;
    if (r < 0.01)
        return 1;

    return 0;
}

static int is_malicious(uint8_t *payload, uint32_t payload_len) {
    for (int i = 0; i < 10; i++) {
        int ridx = random() % str_rule_cnt;
        int ret = match_str(payload, payload_len, str_rule[ridx]);
        if (ret > 0)
            return ridx;
    }

    // N % of packets goes to pcre matching
    double r = (double)rand() / RAND_MAX;
    if (r > 0.2)
        return 0;

    for (int i = 0; i < 20; i++) {
        int ridx = random() % pcre_rule_cnt;
        int ret = match_pcre(payload, payload_len, pcre_rule[ridx]);
        if (ret > 0)
            return ridx;
    }

    return 0;
}

static int init(int param) {
    load_ids_rules();
    connect_future = NULL;
    cluster = cass_cluster_new();
    session = cass_session_new();
    char * hosts = strdup("128.52.162.124,128.52.162.125,128.52.162.131,128.52.162.127,128.52.162.122,128.52.162.120");
    cass_cluster_set_contact_points(cluster, hosts);

    /* Provide the cluster object as configuration to connect the session */
    connect_future = cass_session_connect(session, cluster);

    if (cass_future_error_code(connect_future) == CASS_OK) {


    } else {
        /* Handle error */
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length, message);
    }

    /* Close the session */
    //close_future = cass_session_close(session);
    //cass_future_wait(close_future);
    //cass_future_free(close_future);

    //Closing conntectiong
    //cass_future_free(connect_future);
    //cass_cluster_free(cluster);
    //cass_session_free(session);

    //Query
    /* Build statement and execute query */
//    const char* query = "SELECT release_version FROM system.local";
//    CassStatement* statement = cass_statement_new(query, 0);
//
//    CassFuture* result_future = cass_session_execute(session, statement);
//
//    if (cass_future_error_code(result_future) == CASS_OK) {
//        /* Retrieve result set and get the first row */
//        const CassResult* result = cass_future_get_result(result_future);
//        const CassRow* row = cass_result_first_row(result);
//
//        if (row) {
//            const CassValue* value = cass_row_get_column_by_name(row, "release_version");
//
//            const char* release_version;
//            size_t release_version_length;
//            cass_value_get_string(value, &release_version, &release_version_length);
//            printf("release_version: '%.*s'\n", (int)release_version_length, release_version);
//        }
//
//        cass_result_free(result);
//    } else {
//        /* Handle error */
//        const char* message;
//        size_t message_length;
//        cass_future_error_message(result_future, &message, &message_length);
//        fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
//    }
//
//    cass_statement_free(statement);
//    cass_future_free(result_future);

    return 0;
}
//TODO implement these functions
static void removeFlowFromMap(tcp_hdr * tcph){};

static void flagIpAdress(tcp_hdr * tcph){};

static void updateFlow(tcp_hdr * tcph){};

static int packet_processing(struct rte_mbuf *mbuf) {
    ipv4_hdr *iph = rte_pktmbuf_mtod_offset(mbuf, struct ipv4_hdr *,
    sizeof(struct ether_hdr));

    if (iph->next_proto_id != IPPROTO_TCP)
        return 0;

    tcp_hdr *tcph = (tcp_hdr *)((u_char *)iph +
                                ((iph->version_ihl & IPV4_HDR_IHL_MASK) << 2));

    updateFlow(tcph);
    /* check payload */
    uint8_t *payload = (uint8_t *)tcph + ((tcph->data_off & 0xf0) >> 2);
    uint32_t payload_len = ntohs(iph->total_length) - (payload - (uint8_t *)iph);
    int attack_pattern = 0;

    if (payload_len > 0)
        attack_pattern = is_malicious(payload, payload_len);

    if (attack_pattern > 0) {
        flagIpAdress(tcph);
    }

    if (tcph->tcp_flags & TCP_FLAG_FIN){
        removeFlowFromMap(tcph);
    }


    return 1;  // forward to next hop
};








