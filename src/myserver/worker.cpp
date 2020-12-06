
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, const char * n, const char * order) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "compareprimes");
  req.set_arg("n", oss.str());
  req.set_arg("order", order);
}

// // Implements logic required by compareprimes command via multiple
// // calls to execute_work.  This function fills in the appropriate
// // response.
// static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

//     int params[4];
//     int counts[4];

//     // grab the four arguments defining the two ranges
//     params[0] = atoi(req.get_arg("n1").c_str());
//     params[1] = atoi(req.get_arg("n2").c_str());
//     params[2] = atoi(req.get_arg("n3").c_str());
//     params[3] = atoi(req.get_arg("n4").c_str());

//     for (int i=0; i<4; i++) {
//       Request_msg dummy_req(0);
//       Response_msg dummy_resp(0);
//       create_computeprimes_req(dummy_req, params[i]);
//       execute_work(dummy_req, dummy_resp);
//       counts[i] = atoi(dummy_resp.get_response().c_str());
//     }

//     if (counts[1]-counts[0] > counts[3]-counts[2])
//       resp.set_response("There are more primes in first range.");
//     else
//       resp.set_response("There are more primes in second range.");
// }

#include <pthread.h>
#include "tools/work_queue.h"
#include <map>

typedef struct {
  bool isResp;
  int threadId;
  int numThreads;
} WorkerArgs;

struct fours {
  int cnts[4];
  fours() {
    cnts[0] = -1;
    cnts[1] = -1;
    cnts[2] = -1;
    cnts[3] = -1;
  }
};

static struct Worker_state {
  const static int max_num_tasks = 39;///num_of_thread-1
  WorkQueue<Request_msg> block_queue_tasks;
  pthread_t thread_pool[max_num_tasks];
  WorkerArgs thread_arg[max_num_tasks];
  std::map<int, fours> primes;///tag2compareprimes
} wstate;


void* request_handle(void* thread_arg) {

  WorkerArgs* args = static_cast<WorkerArgs*>(thread_arg);
  Request_msg req;

  while (true) {
    ///try to get a req from the block-queue; block untill it's not empty
    ///and measures are taken for threads-safety
    req = wstate.block_queue_tasks.get_work();
    // Make the tag of the reponse match the tag of the request.  This
    // is a way for your master to match worker responses to requests.
    Response_msg resp(req.get_tag());

    double startTime = CycleTimer::currentSeconds();
    if (req.get_arg("cmd").compare("compareprimes") == 0) {
      // The compareprimes command needs to be special cased since it is
      // built on four calls to execute_execute work.  All other
      // requests from the client are one-to-one with calls to
      // execute_work.
      int order = atoi(req.get_arg("order").c_str());
      req.set_arg("cmd", "countprimes");
      Response_msg dummy_resp(0);
      execute_work(req, dummy_resp);
      wstate.primes[req.get_tag()].cnts[order] = atoi(dummy_resp.get_response().c_str());
      if (order == 0) {
        while (wstate.primes[req.get_tag()].cnts[1] == -1 || 
              wstate.primes[req.get_tag()].cnts[2] == -1 || 
              wstate.primes[req.get_tag()].cnts[3] == -1) continue;

        if (wstate.primes[req.get_tag()].cnts[1]-wstate.primes[req.get_tag()].cnts[0] > 
            wstate.primes[req.get_tag()].cnts[3]-wstate.primes[req.get_tag()].cnts[2])
          resp.set_response("There are more primes in first range.");
        else
          resp.set_response("There are more primes in second range.");
        wstate.primes.erase(wstate.primes.find(req.get_tag()));
        args->isResp = true;
      } else {
        args->isResp = false;
      }
    } else {
      // actually perform the work.  The response string is filled in by
      // 'execute_work'
      execute_work(req, resp);
      args->isResp = true;
    }

    double dt = CycleTimer::currentSeconds() - startTime;
    DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";
    if (args->isResp) {
      // send a response string to the master
      worker_send_response(resp);
    }
  }
  return NULL;
}

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  for (int i = 0; i < wstate.max_num_tasks; i++) {
    wstate.thread_arg[i].threadId = i + 1;
    wstate.thread_arg[i].numThreads = wstate.max_num_tasks;
    pthread_create(&wstate.thread_pool[i], NULL, request_handle, &wstate.thread_arg[i]);
  }

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

  // // wait for worker threads to complete??
  // for (int i=1; i<numThreads; i++)
  //   pthread_join(workers[i], NULL);
}

void worker_handle_request(const Request_msg& req) {

  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  if (req.get_arg("cmd") == "compareprimes") {
    ///divide to four independent task
    Request_msg dummy_req(req.get_tag());
    ///initialize the four-counts
    wstate.primes[req.get_tag()] = fours();
    // grab the four arguments defining the two ranges
    create_computeprimes_req(dummy_req, req.get_arg("n1").c_str(), "0");
    wstate.block_queue_tasks.put_work(dummy_req);
    create_computeprimes_req(dummy_req, req.get_arg("n2").c_str(), "1");
    wstate.block_queue_tasks.put_work(dummy_req);
    create_computeprimes_req(dummy_req, req.get_arg("n3").c_str(), "2");
    wstate.block_queue_tasks.put_work(dummy_req);
    create_computeprimes_req(dummy_req, req.get_arg("n4").c_str(), "3");
    wstate.block_queue_tasks.put_work(dummy_req);
  } else {
    ///add the request to block-queue and it would be handled by other threads
    wstate.block_queue_tasks.put_work(req);
  }

}
