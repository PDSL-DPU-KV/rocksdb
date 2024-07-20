#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

class ThreadPool {
#ifdef  TASKLIST
    struct task_node {
        std::function<void()> task;
        task_node* next;
        int priority;
        std::chrono::high_resolution_clock::time_point task_time;
    };
#endif  //
public:
    ThreadPool(size_t);
    int get_tid(std::thread::id);
    template <class F, class... Args>
    auto enqueue(F&& f, Args &&...args)
        -> std::future<typename std::result_of<F(Args...)>::type>;
    ~ThreadPool();

private:
    // need to keep track of threads so we can join them
    std::vector<std::thread> workers;
    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
    // store thread_id
    std::unordered_map<std::thread::id, int> tids;
#ifndef  TASKLIST
    // the task queue
    std::queue<std::function<void()>> tasks;
#else
    task_node* node_head;
    bool empty() {
        if (node_head->next == nullptr) {
            return true;
        }
        return false;
    }
    task_node* get_front() {
        task_node* result = node_head->next;
        node_head->next = result->next;
        return result;
    }
    void emplace(task_node* add_task_node) {
        task_node* temp = node_head;
        auto time_now = std::chrono::high_resolution_clock::now();
        while (temp->next != nullptr) {
            if (std::chrono::duration_cast<std::chrono::seconds>(time_now - temp->next->task_time).count()+temp->next->priority
                <add_task_node->priority) {
                add_task_node->next = temp->next;
                temp->next = add_task_node;
                return;
            }
            temp = temp->next;
        }
        temp->next = add_task_node;
        add_task_node->next = nullptr;
        return;
    }
    void printList() {
      task_node* temp = node_head;
      auto time_now = std::chrono::high_resolution_clock::now();
      int cnt = 0;
      while (temp->next != nullptr) {
        temp = temp->next;
        printf("TaskID:%d Tneed:%d Twait:%d\n",cnt++,temp->priority,std::chrono::duration_cast<std::chrono::seconds>(time_now - temp->task_time).count());
      }
    }
#endif

};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads) : stop(false) {
    #ifdef  TASKLIST
        node_head = new task_node{std::function<void()>(), nullptr,0,std::chrono::high_resolution_clock::now()};
    #endif  //
    for (size_t i = 0; i < threads; ++i) {
        workers.emplace_back([this] {
            for (;;) {
                #ifdef  TASKLIST
                    task_node* pointer = nullptr;
                #endif  //
                std::function<void()> task;

                {
                    std::unique_lock<std::mutex> lock(this->queue_mutex);
                    #ifndef TASKLIST
                        this->condition.wait(
                            lock, [this] { return this->stop || !this->tasks.empty(); });
                        if (this->stop && this->tasks.empty())
                            return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    #else
                        this->condition.wait(
                            lock, [this] { return this->stop || !this->empty(); });
                        if (this->stop && this->empty()) return;
                        pointer = this->get_front();
                        task = pointer->task;
                    #endif
                }
                task();
                #ifdef  TASKLIST
                    delete pointer;
                #endif  //
            }
        });
        tids[workers[i].get_id()] = i;
    }
}

// get tid
int ThreadPool::get_tid(std::thread::id id) { return tids[id]; }

// add new work item to the pool
template <class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args &&...args)
-> std::future<typename std::result_of<F(Args...)>::type> {
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        #ifndef TASKLIST
        if (stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");
        tasks.emplace([task]() { (*task)(); });
        #else
        if (stop) throw std::runtime_error("enqueue on stopped ThreadPool");
        char* char_pointer =
            std::get<0>(std::forward_as_tuple(std::forward<Args>(args)...));
        // 生成随机整数
        int priority = std::rand() % 100 + 1;  // int priority = v/h      -(p-1)*v/k;
        // int priority = *(int*)char_pointer;
        task_node* add_task = new task_node{[task]() { (*task)(); }, nullptr,priority,std::chrono::high_resolution_clock::now()};// TODO
        this->emplace(add_task);
        // printList();
        #endif
        
    }
    condition.notify_one();
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread& worker : workers)
        worker.join();
}

#endif