/*
* Using Boost library to run fixed amount of threads with limited resources, threads will share resource efficiently.
* Reference : https://www.boost.org/doc/libs/1_66_0/doc/html/interprocess/synchronization_mechanisms.html

  g++ MoreThrdsLimitedResources_Semaphore.cpp -lboost_thread -lboost_system -lrt -pthread -lpq

* Problem set is , we need to build indexes and PK for table in postgres DB. 
* We have certain resources (3 libpq connections). These connections can be executed in parallel using threads
* Create fixed number of threads greater than the resources, and they need to share limited resources(libpq connection)
* Threads can execute in random and they should hold on to single connection to execute task.
* If all connections are busy and not available for thread, it should wait till connection becomes available. 
* We need synchronization of threads to get limited resource(libpq connection), this can be achieved using semaphore.
* Sempahore should have same value as number of resources.
* Also Semaphore wait and post functions make sure it waits until resource is available, no need for condition variable. 
* To create fixed number of threads and call functions using asio library.
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
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include<set>

using namespace std;

PGresult   *res;
ExecStatusType returnStatus;
boost::mutex updateMutex;
boost::interprocess::interprocess_semaphore connectionSemaphore(3);
string const conninfo = "dbname='1782_MT_TNSFM' host='localhost' user='admin' password='admin'";
vector<PGconn *> connections(3); // 3 resources (connections), that need to be shared by threads.
set<int> connectionsAvailable;

void executeCommand(string &copyCommand){
    res = NULL;
    int connectionNumber = -1;
    PGconn *localconn = nullptr;
    connectionSemaphore.wait(); // wait to get semaphore resource.. 
    {
        boost::mutex::scoped_lock updateScopedLock(updateMutex); // lock mutex for getting connection 
        connectionNumber = *(connectionsAvailable.begin()); // get index of connection to use for this task
        connectionsAvailable.erase(connectionNumber); // make sure entry of index is deleted so next threads in queue should not get this connection 
        localconn = connections[connectionNumber];
        cout << "Start Thread:" << boost::this_thread::get_id() << ", pgConn :"  << localconn << " execCmd: " << copyCommand << endl;
    }
    
    res = PQexec(localconn, copyCommand.c_str());
    returnStatus = PQresultStatus(res);
    if ( returnStatus == PGRES_FATAL_ERROR || returnStatus == PGRES_NONFATAL_ERROR ) {
        printf("Copy command failed with return status: %d, messg: %s \n", returnStatus, PQresultErrorMessage(res));
        PQclear(res);
        return ;
    }
    PQclear(res);
    {
        boost::mutex::scoped_lock updateScopedLock(updateMutex);
        connectionsAvailable.insert(connectionNumber);
        cout << "Finish Thread:" << boost::this_thread::get_id() << endl;
    }
    connectionSemaphore.post(); // return resource to next threads.
}

void multiThreadIndexing(vector<string> &vec){
    size_t number_of_threads = boost::thread::hardware_concurrency();
    cout << "Running multi threaded indexing, threads :  "<< number_of_threads << endl;
    boost::asio::io_service io_service;
    boost::thread_group threads;
    boost::asio::io_service::work *work = new boost::asio::io_service::work(io_service);

    for(size_t t = 0; t < number_of_threads; t++){
        boost::thread *thr = threads.create_thread(boost::bind(&boost::asio::io_service::run, &io_service)); // create pool of threads
    }

    size_t size = vec.size();
    string copyCommand;
    for (size_t i=0; i < size; i++ ){
        copyCommand = vec[i];
        io_service.post(boost::bind(executeCommand, copyCommand)); // fixed number of threads will execute all needed functions.
    }
        
    delete work;
    threads.join_all(); // wait for all jobs to finish..
    io_service.stop(); // stop threadpool service.. 
}


int main(int argc, char **argv)
{
    vector<string> vec; 
    vec.push_back("ALTER TABLE \"dbo\".\"ORDERS\" ADD  PRIMARY KEY (\"O_ORDERKEY\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_CUSTKEY_IDX\" on \"dbo\".\"ORDERS\"(\"O_CUSTKEY\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_ORDERSTATUS_IDX\" on \"dbo\".\"ORDERS\"(\"O_ORDERSTATUS\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_TOTALPRICE_IDX\" on \"dbo\".\"ORDERS\"(\"O_TOTALPRICE\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_ORDERDATE_IDX\" on \"dbo\".\"ORDERS\"(\"O_ORDERDATE\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_ORDERPRIORITY_IDX\" on \"dbo\".\"ORDERS\"(\"O_ORDERPRIORITY\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_CLERK_IDX\" on \"dbo\".\"ORDERS\"(\"O_CLERK\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_SHIPPRIORITY_IDX\" on \"dbo\".\"ORDERS\"(\"O_SHIPPRIORITY\");");
    vec.push_back("CREATE INDEX \"ORDERS_O_COMMENT_IDX\" on \"dbo\".\"ORDERS\"(\"O_COMMENT\");");

    
    for(int i=0; i<3; i++){
        PGconn *conn = PQconnectdb(conninfo.c_str());
        if (PQstatus(conn) != CONNECTION_OK)
        {
            fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
            PQfinish(conn);
            return -1;
        }
        connections[i] = conn; // store PG connections in vector to access correctly 
        connectionsAvailable.insert(i); // insert index of connection in set to thread can pick it up correctly. 
    }

    multiThreadIndexing(vec);

    for(int i=0; i<3; i++){
        //cout << " Closing pg conn :" << connections[i] << endl; 
        PQfinish(connections[i]); // once thread jobs are over, free the resources ( libpq connection)
    }
    
    return 0;
}
