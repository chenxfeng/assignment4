#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>

#include "server/messages.h"
#include "server/master.h"

#include <map>
#include <vector>
#include <sstream>
#include <set>

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int num_pending_client_requests;
  int next_tag;
  ///handle multiple pending client requests
  std::map<int, Client_handle> waiting_client;///tag2client
  ///multi workers
  int start_num_workers;
  std::vector<Worker_handle> my_worker;
  std::map<Worker_handle, int> worker_num;
  ///load balance
  int next_worker;
  ///load info
  std::map<Worker_handle, int> workers_load;
  std::set<std::pair<int, int> > sorted_worker;
  ///Elasticity
  int threshold;
  int start_worker_req;
  int low_bound;
  bool end_worker_req;

} mstate;

void update_next_worker(const char* manner = "least connection") {
  ///in C++, using ``==`` to compare two string is valid, but
  ///comparison with string literal results in unspecified behaviour in C 
  if (strcmp(manner, "round robin") == 0) {
    ///round-robin's manner
    mstate.next_worker = (mstate.next_worker + 1) % mstate.my_worker.size();
  } else if (strcmp(manner, "random") == 0) {
    ///random manner
    mstate.next_worker = random() % mstate.my_worker.size();
  } else if (strcmp(manner, "least connection") == 0) {
    ///Least Connections
    auto it = mstate.sorted_worker.begin();
    mstate.next_worker = it->second;
  }
  ///and more...
  ///last worker will be killed
  if (mstate.end_worker_req && mstate.next_worker == mstate.my_worker.size() - 1)
    mstate.next_worker --;
}

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 1;//5;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;
  mstate.next_worker = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker
  mstate.start_num_workers = mstate.max_num_workers - 2;//1;
  for (int i = 0; i < mstate.start_num_workers; ++i) {
    int tag = random();
    Request_msg req(tag);
    std::ostringstream oss;
    oss << "my worker " << i;
    req.set_arg("name", oss.str());
    request_new_worker_node(req);
  }
  ///initialize elasticity-argus
  mstate.threshold = 7;
  mstate.start_worker_req = 0;
  mstate.low_bound = 2;
  mstate.end_worker_req = false;
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.
  printf("start worker-%lu\n", mstate.my_worker.size());
  mstate.my_worker.push_back(worker_handle);
  mstate.worker_num[worker_handle] = mstate.my_worker.size() - 1;
  mstate.sorted_worker.insert(std::pair<int, int>(0, mstate.worker_num[worker_handle]));
  mstate.workers_load[worker_handle] = 0;

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
  ///elasticity: a start-worker req done
  if (mstate.start_worker_req > 0)
    mstate.start_worker_req --;
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  mstate.sorted_worker.erase(
    mstate.sorted_worker.find(std::pair<int, int>(
    mstate.workers_load[worker_handle], mstate.worker_num[worker_handle])));
  mstate.workers_load[worker_handle] --;
  mstate.sorted_worker.insert(std::pair<int, int>(
    mstate.workers_load[worker_handle], mstate.worker_num[worker_handle]));

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  int tag = resp.get_tag();
  Client_handle client = mstate.waiting_client[tag];
  send_client_response(client, resp);

  mstate.num_pending_client_requests --;
  mstate.waiting_client.erase(mstate.waiting_client.find(tag));
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.
  int tag = mstate.next_tag++;
  Request_msg worker_req(tag, client_req);
  send_request_to_worker(mstate.my_worker[mstate.next_worker], worker_req);
  ///update load info
  Worker_handle& worker_handle = mstate.my_worker[mstate.next_worker];
  mstate.sorted_worker.erase(
    mstate.sorted_worker.find(std::pair<int, int>(
    mstate.workers_load[worker_handle], mstate.worker_num[worker_handle])));
  mstate.workers_load[worker_handle] ++;
  mstate.sorted_worker.insert(std::pair<int, int>(
    mstate.workers_load[worker_handle], mstate.worker_num[worker_handle]));
  ///update next_worker
  update_next_worker();

  // Save off the handle to the client that is expecting a response.
  // The master needs to do this it can response to this client later
  // when 'handle_worker_response' is called.
  mstate.waiting_client[tag] = client_handle;
  mstate.num_pending_client_requests++;

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.
}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.
  ///start new worker when the least load succeed a threshold
  if (mstate.sorted_worker.begin()->first > mstate.threshold && 
    mstate.my_worker.size() + mstate.start_worker_req < mstate.max_num_workers) {
    ///elasticity: a start-worker req
    mstate.start_worker_req ++;
    printf("start %d-th worker when least load is %d\n", 
      mstate.my_worker.size()+mstate.start_worker_req, mstate.sorted_worker.begin()->first);

    int tag = random();
    Request_msg req(tag);
    std::ostringstream oss;
    oss << "my worker " << mstate.my_worker.size();
    req.set_arg("name", oss.str()); 
    request_new_worker_node(req);
  }
  if (mstate.end_worker_req && mstate.workers_load[mstate.my_worker.back()] == 0) {
    kill_worker_node(mstate.my_worker.back());
    mstate.sorted_worker.erase(mstate.sorted_worker.find(
      std::pair<int, int>(mstate.workers_load[mstate.my_worker.back()], 
      mstate.worker_num[mstate.my_worker.back()])));
    mstate.workers_load.erase(mstate.workers_load.find(mstate.my_worker.back()));
    mstate.worker_num.erase(mstate.worker_num.find(mstate.my_worker.back()));
    mstate.my_worker.pop_back();
    mstate.end_worker_req = false;
  }
  if (!mstate.end_worker_req && mstate.sorted_worker.size()) {
    auto it = mstate.sorted_worker.end();
    --it;
    if (it->first <= mstate.low_bound && mstate.my_worker.size() > 1) {
      mstate.end_worker_req = true;
    }
  }
}

