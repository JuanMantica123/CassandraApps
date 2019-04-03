//
// Created by juan on 4/2/19.
//

#ifndef CASSANDRAAPPLICATIONS_CASSANDRA_MANAGER_H
#define CASSANDRAAPPLICATIONS_CASSANDRA_MANAGER_H

#include <stdio.h>
#include <memory.h>
#include "cassandra.h"


char * hosts = strdup("128.52.162.124,128.52.162.125,128.52.162.131,128.52.162.127,128.52.162.122,128.52.162.120");

void print_error(CassFuture* future) {
    const char* message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

static CassCluster* create_cluster() {
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, hosts);
    return cluster;
}

static CassError connect_session(CassSession* session, const CassCluster* cluster) {
    CassError rc = CASS_OK;
    CassFuture* future = cass_session_connect(session, cluster);

    cass_future_wait(future);
    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        print_error(future);
    }
    cass_future_free(future);

    return rc;
}



//int main(int argc, char* argv[]) {
//    CassCluster* cluster = NULL;
//    CassSession* session = cass_session_new();
//    char* hosts = "127.0.0.1";
//
//    if (argc > 1) {
//        hosts = argv[1];
//    }
//    cluster = create_cluster();
//
//    if (connect_session(session, cluster) != CASS_OK) {
//        cass_cluster_free(cluster);
//        cass_session_free(session);
//        return -1;
//    }
//
//    execute_query(session,
//                  "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = { "
//                  "'class': 'SimpleStrategy', 'replication_factor': '3' };");
//
//
//    execute_query(session,
//                  "CREATE TABLE IF NOT EXISTS examples.duration "
//                  "(key text PRIMARY KEY, d duration)");
//
//    /* Insert some rows into the table and read them back out */
//
//    insert_into(session, "zero", 0, 0, 0);
//    insert_into(session, "one_month_two_days_three_seconds", 1, 2, 3 * NANOS_IN_A_SEC);
//    insert_into(session, "negative_one_month_two_days_three_seconds", -1, -2, -3 * NANOS_IN_A_SEC);
//
//    select_from(session, "zero");
//    select_from(session, "one_month_two_days_three_seconds");
//    select_from(session, "negative_one_month_two_days_three_seconds");
//
//    cass_cluster_free(cluster);
//    cass_session_free(session);
//
//    return 0;
//}

#endif //CASSANDRAAPPLICATIONS_CASSANDRA_MANAGER_H
