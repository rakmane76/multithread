/*
* Using Boost asio library to make fixed amount of threads process certain tasks. 
* https://stackoverflow.com/questions/31835009/c-threadpool-is-not-running-parallel/31835802#31835802
* http://think-async.com/Asio/Recipes
*
* Also how do we assign certain resources to thread, that are specific to that thread only. 
* This will help perfrom better rather than wanting threads to share the resource. 
* Point was how to make sure thread is assigned specific resource from pool of resource. 
*
*/
// g++ libpq_createIndex.cpp -lboost_thread -lboost_system -lrt -pthread -lpq
//
/*
* Problem set is , we need to build indexes and PK for table in postgres DB. 
* We have certain resources ( 4 libpq connections)
* Need to create threads , assign each thread with specific libpq connection , so it doesnt have to wait.
* Execute sql commands in parallel using threads. 
* Note sql commands can be more than number of threads, how do we manage threads to execute all commands and then kill threads.. 
*
*/
#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <libpq-fe.h>
#include<vector>
#include<iostream>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include<map>

using namespace std;

PGconn     *conn;
PGresult   *res;
ExecStatusType returnStatus;
map<boost::thread::id, PGconn *> mapConn; // mapping of thread with resource.. 
//boost::mutex printMutex;

void executeCommand(string &copyCommand){
    res = NULL;

    PGconn  *localconn = mapConn[boost::this_thread::get_id()]; // get specific resource , ( libpq connection) assigned for thread
    if (PQstatus(localconn) != CONNECTION_OK){
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        return;
    }
    
    /*printMutex.lock();
    cout << "In thread id: " << boost::this_thread::get_id() << ", pgConn :"  << localconn << " execCmd: " << copyCommand << endl;
    printMutex.unlock();*/

    res = PQexec(localconn, copyCommand.c_str());
    returnStatus = PQresultStatus(res);
    if ( returnStatus == PGRES_FATAL_ERROR || returnStatus == PGRES_NONFATAL_ERROR ) {
        printf("Copy command failed with return status: %d, messg: %s \n", returnStatus, PQresultErrorMessage(res));
        PQclear(res);
        return ;
    }
    PQclear(res);
}

void multiThreadIndexing(vector<string> &vec, string conninfo){
    size_t number_of_threads = 3;
    cout << "Running multi threaded indexing, threads :  "<< number_of_threads << endl;
    boost::asio::io_service io_service;
    boost::thread_group threads;
    boost::asio::io_service::work *work = new boost::asio::io_service::work(io_service);

    for(size_t t = 0; t < number_of_threads; t++){
        boost::thread *thr = threads.create_thread(boost::bind(&boost::asio::io_service::run, &io_service));
        PGconn *lconn = PQconnectdb(conninfo.c_str());
        if (PQstatus(conn) != CONNECTION_OK)
        {
            fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
            PQfinish(conn);
            return;
        }
        //cout << "Created map , thread :" << thr->get_id() << ", pgconn :" << lconn << endl;
        mapConn[thr->get_id()] = lconn; // assign resource (libpq conn) to thread.. 
    }

    size_t size = vec.size();
    string copyCommand;
    for (size_t i=0; i < size; i++ ){
        copyCommand = vec[i];
        io_service.post(boost::bind(executeCommand, copyCommand));
    }
        
    delete work;
    threads.join_all(); // wait for all jobs to finish..
    io_service.stop(); // stop threadpool service.. 
}


void singleThreadIndexing(vector<string> &vec, PGconn *conn){
    res = NULL;
    size_t size = vec.size();
    cout << "Running single threaded indexing " << endl;
    string copyCommand;
    for (size_t i=0; i < size; i++ ){
        copyCommand = vec[i];
        //cout << "Command exec: " << copyCommand << endl;
        res = PQexec(conn, copyCommand.c_str());
        returnStatus = PQresultStatus(res);
        if ( returnStatus == PGRES_FATAL_ERROR || returnStatus == PGRES_NONFATAL_ERROR ) {
            printf("Copy command failed with return status: %d, messg: %s \n", returnStatus, PQresultErrorMessage(res));
            PQclear(res);
            return ;
        }
        PQclear(res);
    }
}

int main(int argc, char **argv)
{
    vector<string> vec; // contains all tasks. sql commands to be executed. 
    vec.push_back("ALTER TABLE \"dbo\".\"ORDERS\" ADD  PRIMARY KEY (\"O_ORDERKEY\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_CUSTKEY_IDX\" on \"dbo\".\"ORDERS\"(\"O_CUSTKEY\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_ORDERSTATUS_IDX\" on \"dbo\".\"ORDERS\"(\"O_ORDERSTATUS\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_TOTALPRICE_IDX\" on \"dbo\".\"ORDERS\"(\"O_TOTALPRICE\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_ORDERDATE_IDX\" on \"dbo\".\"ORDERS\"(\"O_ORDERDATE\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_ORDERPRIORITY_IDX\" on \"dbo\".\"ORDERS\"(\"O_ORDERPRIORITY\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_CLERK_IDX\" on \"dbo\".\"ORDERS\"(\"O_CLERK\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_SHIPPRIORITY_IDX\" on \"dbo\".\"ORDERS\"(\"O_SHIPPRIORITY\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_COMMENT_IDX\" on \"dbo\".\"ORDERS\"(\"O_COMMENT\");");


    int flag{}; 
    if ( argc > 1 ) flag = atoi(argv[1]);

    string conninfo = "dbname='DB_NAME' host='localhost' user='USER' password='PASSWD'";

    conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return (-1);
    }
    else
    {
        if(flag == 1) {
            multiThreadIndexing(vec, conninfo);
            map<boost::thread::id, PGconn *>::iterator it = mapConn.begin();
            for ( ; it != mapConn.end(); it++) {
                //cout << " Closing pg conn :" << it->second << endl; 
                PQfinish(it->second); // once thread jobs are over, free the resources ( libpq connection)
            }
        }
        else singleThreadIndexing(vec, conn);
        PQfinish(conn); // close connection 
    }
    
    return 0;
}
