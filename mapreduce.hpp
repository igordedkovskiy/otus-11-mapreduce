#pragma once

#include <cstdint>
#include <vector>
#include <list>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <functional>
#include <thread>

template<typename T> struct TD;

namespace mapreduce
{

struct Block
{
    std::size_t m_start{0};
    std::size_t m_end{0};
};

// Это MapReduce фреймворк. Он универсальный и ничего не знает о задаче, которую решает.
// Здесь не должно быть кода, завязанного на конкретную задачу.
//
// С помощью этого фреймворка должны решаться разные задачи.
// Когда напишете это, попробуйте решить с помощью этого фреймворка все задачи, которые мы разбирали
// на лекции.
//
// Это наш самописный аналог hadoop mapreduce.
// Он на самом деле не работает с по-настоящему большими данными, потому что выполняется на одной
// машине. Но мы делаем вид, что данных много, и представляем, что наши потоки - это процессы на
// разных узлах.
//
// Ни один из потоков не должен полностью загружать свои данные в память или пробегаться по всем
// данным. Каждый из потоков обрабатывает только свой блок.
//
// На самом деле даже один блок данных не должен полностью грузиться в оперативку, а должен
// обрабатываться построчно. Но в домашней работе можем этим пренебречь и загрузить один блок в
// память одним потоком.

//template<typename MapperT, typename ReducerT, typename KeyT>
class Framework
{
public:
    using KeyT = std::string;//std::size_t;
    using pair_t = std::pair<KeyT, std::size_t>;
    using pairs_t = std::list<pair_t>;
    using blocks_of_pairs_t = std::vector<pairs_t>;

    using MapperT = void(const std::filesystem::path&, const mapreduce::Block&, pairs_t&);
    using ReducerT = void(const pairs_t&, pairs_t&);

public:
    template<typename M, typename R>
    Framework(const M& mapper, std::size_t num_of_mappers,
              const R& reducer, std::size_t num_of_reducers);

    void run(const std::filesystem::path& input, const std::filesystem::path& output);

    template<typename M> void set_mapper(const M& mapper);
    template<typename R> void set_reducer(const R& reducer);

private:
    using input_blocks_t = std::vector<Block>;
    input_blocks_t split_input(const std::filesystem::path& file_path);

    /// \returns num_of_mappers vectors of pairs
    blocks_of_pairs_t map(const std::filesystem::path& fpath, const input_blocks_t& blocks);

    /// \returns num_of_reducers vectors of pairs
    blocks_of_pairs_t shuffle(blocks_of_pairs_t& mapped);

    pairs_t reduce(const blocks_of_pairs_t& shuffled);

    std::size_t m_num_of_mappers{0};
    std::size_t m_num_of_reducers{0};

    std::function<MapperT>  m_mapper;
    std::function<ReducerT> m_reducer;
};

template<typename M, typename R>
Framework::Framework(const M& mapper, std::size_t num_of_mappers,
                     const R& reducer, std::size_t num_of_reducers):
    //m_mapper{std::forward<decltype(mapper)>(mapper)},
    m_mapper{mapper},
    m_num_of_mappers{num_of_mappers},
    //m_reducer{std::forward<decltype(reducer)>(reducer)},
    m_reducer{reducer},
    m_num_of_reducers{num_of_reducers}
{}

template<typename M> void Framework::set_mapper(const M& mapper)
{
    m_mapper = mapper;
}

template<typename R> void Framework::set_reducer(const R& reducer)
{
    m_reducer = reducer;
}



}
