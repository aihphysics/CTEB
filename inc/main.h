
#include <type_traits>
#include <atomic>
#include <optional>
#include <tuple>
#include <concepts>
#include <memory>
#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>

struct msg1 { int i; float j; };
struct sub1 {
  bool accepted = false;
  using MessageType = msg1;
  void process(msg1&& msg){ accepted = true; }
};

struct msg2 { int i; float j; };
struct sub2 {
  bool accepted = false;
  using MessageType = msg2;
  void process(msg2&&){ accepted = true; }
};

// this could probably be rewritten to use conditionals on std::get
template<std::size_t idx, std::size_t len, typename target, typename tuple>
struct tupleTypeIndex;

// miss case
template<std::size_t idx, std::size_t len, typename target, typename tail>
struct tupleTypeIndex<idx, len, target, std::tuple<tail>> {
  static constexpr std::size_t value = -1;
};

// match case
template<std::size_t idx, std::size_t len, typename target, typename...tail>
struct tupleTypeIndex<idx, len, target, std::tuple<target, tail...>>{
  static constexpr std::size_t value = len-idx;
};

// match case
template<std::size_t idx, std::size_t len, typename target>
struct tupleTypeIndex<idx, len, target, std::tuple<target>> {
  static constexpr std::size_t value = len-idx;
};

// recur case
template<std::size_t idx, std::size_t len, typename target, typename head, typename...tail>
struct tupleTypeIndex<idx, len, target, std::tuple<head, tail...>> 
  : tupleTypeIndex<idx-1, len, target, std::tuple<tail...>>
{};

template<typename T>
concept Messageable = requires (T t, T::MessageType m){
  typename T::MessageType;
  t.process(std::move(m));
};

// consumer, propogates data to a 
template<Messageable T>
class Consumer {

  using MessageType = T::MessageType;

  private:
    std::queue<MessageType> data_queue = {};
    std::mutex mutex;
    std::condition_variable cv;
    std::thread thread;
    std::atomic<bool> running;

  public:

    Consumer(std::shared_ptr<T> messageable)
    {

      thread = std::thread( [this, messageable = std::move(messageable)] {

        std::unique_lock<std::mutex> lock = std::unique_lock(mutex);
        
        // start barrier
        cv.wait(lock, [this]{return running.load(); });

        while (running.load()) {

          while (running.load() && !data_queue.empty()) {
            auto data = std::move(data_queue.front());
            data_queue.pop();

            lock.unlock();
            messageable->process(std::move(data));
            lock.lock();
          }
          cv.wait(lock, [this]{return !running.load() || !data_queue.empty(); });
        }
      });
    }

    ~Consumer(){
      running.store(false);
      cv.notify_one();
      thread.join();
    }

    void start(){
      running.store(true);
    }
    
    // allows in-place construction of matching messages.
    template<typename...V>
    void queue(V&&...data){
      {
        auto lk = std::unique_lock<std::mutex>(mutex);
        data_queue.emplace(std::forward<V>(data)...);
      }
      cv.notify_one();
    };

};

template<typename target, typename...elems>
struct getIdx;

template<typename target, typename...elems>
struct getIdx<target, std::tuple<std::optional<Consumer<elems>>...>> 
  : tupleTypeIndex<
      std::tuple_size_v<std::tuple<elems...>>, 
      std::tuple_size_v<std::tuple<elems...>>, 
      target, 
      std::tuple<elems...>
    >
{};

template<typename target, typename...elems>
constexpr bool consumerInBus() {
  return getIdx<target, elems...>::value != -1;
};


template<typename T>
concept ConsumerType = std::is_constructible_v<Consumer<T>,std::shared_ptr<T>>;

template<ConsumerType...T>
class Bus {
  std::tuple<std::optional<Consumer<T>>...> subscribers;
  std::atomic<bool> started = false;

  public:
    
    template<
      ConsumerType U, 
      std::enable_if_t<consumerInBus<U,decltype(subscribers)>(),bool> = true
    > 
    void subscribe(std::shared_ptr<U> subscriber){
      std::get<std::optional<Consumer<U>>>(subscribers).emplace(std::move(subscriber));
    }
    
    void start(){

      // are the consumers populated?
      bool ready = std::apply( 
        [&](std::optional<Consumer<T>>&...sub){
          return ( sub.has_value() && ... );
        },
        subscribers
      );

      // if not, RBD
      if (!ready) {
        throw std::runtime_error("Bus has uninitialized consumers");
      }

      // else, start all consumers.
      std::apply( 
          [&](std::optional<Consumer<T>>&...sub){
            ( sub->start(), ... );
          },
          subscribers
      );
      started.store(true);
    }


    template<
      ConsumerType U, 
      typename...V,
      std::enable_if_t<std::is_constructible_v<typename U::MessageType, V...>,bool> = true,
      std::enable_if_t<consumerInBus<U,decltype(subscribers)>(),bool> = true
    >
    void message(V&&...data) {
      if (!started.load()) {
        throw std::runtime_error("Bus has not been started.");
      }
      auto& opt = std::get<std::optional<Consumer<U>>>(subscribers);
      opt->queue(std::forward<V>(data)...);
    }


    ~Bus(){
      // reset, call destructors on all consumers, bring down the queues.
      std::apply( 
        [&](std::optional<Consumer<T>>&...sub){
          ( sub.reset(), ... );
        },
        subscribers
      );
    }

};
