
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

struct msg1 {
  int i;
  float j;
};

class sub1 {

  public:
    bool accepted = false;

    using MessageType = msg1;
    void process(msg1&& msg){
      accepted = true;
    }
};

struct msg2 {
  int i;
  float j;
};

class sub2 {

  public:
    bool accepted = false;

    using MessageType = msg2;
    void process(msg2&&){
      accepted = true;
    }
};


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

template<typename target, typename...elems>
struct getIdx;

template<typename target, typename...elems>
struct getIdx<target, std::tuple<elems...>> 
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

      running.store(true);
      thread = std::thread( [this, messageable = std::move(messageable)] {

        std::unique_lock<std::mutex> lock = std::unique_lock(mutex);
        while ( running.load() ) {

          while ( running.load() && !data_queue.empty() ){
            auto data = std::move(data_queue.front() );
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

    
    // allows in-place construction of matching messages. -> maybe inadvisable?
    template<typename...V>
    void queue(V&&...data){
      {
        auto lk = std::unique_lock<std::mutex>(mutex);
        data_queue.emplace(std::forward<V>(data)...);
      }
      cv.notify_one();
    };

};

template<typename T>
concept ConsumerType = std::is_constructible_v<Consumer<T>,std::shared_ptr<T>>;

template<ConsumerType...T>
class Bus
{

  std::tuple<std::optional<Consumer<T>>...> subscribers;

  public:
    //template<
    //  typename U, 
    //  std::enable_if_t<consumerInBus<U,decltype(subscribers)>(),bool> = true
    //>
    //void subscribe(U&& u){
    //  std::get<getIdx<U,decltype(subscribers)>::value>(subscribers) = std::move(u);
    //}
    
    template<ConsumerType U>
    void subscribe(std::shared_ptr<U> subscriber){
      //std::optional<Consumer<U>> val = std::make_optional<Consumer<U>>(std::move(subscriber));
      //std::get<std::optional<Consumer<U>>>(subscribers) = std::make_optional<Consumer<U>>(std::move(subscriber));
      std::get<std::optional<Consumer<U>>>(subscribers).emplace(std::move(subscriber));
    }

    template<
      ConsumerType U, 
      typename...V,
      std::enable_if_t<std::is_constructible_v<typename U::MessageType, V...>,bool> = true
    >
    void message(V&&...data) {
      auto& opt = std::get<std::optional<Consumer<U>>>(subscribers);
      opt->queue(std::forward<V>(data)...);
    }

    ~Bus(){
      std::apply( 
        [&](std::optional<Consumer<T>>&...sub){
          ( sub.reset(), ... );
        },
        subscribers
      );

    }

};
