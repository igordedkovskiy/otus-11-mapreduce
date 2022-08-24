#pragma once

#include <cstdint>
#include <vector>
#include <list>
#include <map>
#include <filesystem>
#include <fstream>
#include <functional>
#include <thread>
#include <exception>
#include <iostream>

template<typename T> struct TD;

namespace mapreduce
{

struct Block
{
    std::size_t m_start{0};
    std::size_t m_end{0};
};

class Framework
{
public:
    using KeyT = std::string;
    using pair_t = std::pair<KeyT, std::size_t>;
    //using pairs_t = std::list<pair_t>;
    using pairs_t = std::multimap<KeyT, std::size_t>;
    using blocks_of_pairs_t = std::vector<pairs_t>;

    using MapperT = void(const std::filesystem::path&, const mapreduce::Block&, pairs_t&);
    using ReducerT = void(const pairs_t&, pairs_t&);

    struct exception: public std::exception
    {
        exception(const std::string& m);
        exception(std::string&& m) noexcept;
        const char* what() const noexcept;
        std::string m_message;
    };

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

    std::function<MapperT>  m_mapper;
    std::size_t m_num_of_mappers{0};
    std::function<ReducerT> m_reducer;
    std::size_t m_num_of_reducers{0};
};

template<typename M, typename R>
Framework::Framework(const M& mapper, std::size_t num_of_mappers,
                     const R& reducer, std::size_t num_of_reducers):
    m_mapper{mapper},
    m_num_of_mappers{num_of_mappers},
    m_reducer{reducer},
    m_num_of_reducers{num_of_reducers}
{
    if(m_num_of_reducers > m_num_of_mappers)
    {
        m_num_of_reducers = m_num_of_mappers;
        std::cerr << "Entered number of reducers is greater than number of mappers."
                     "Therefore number of reducers is set equal to number of mappers" << std::endl;
//        throw exception("Number of reducers is greater than number of mappers");
    }
}

template<typename M> void Framework::set_mapper(const M& mapper)
{
    m_mapper = mapper;
}

template<typename R> void Framework::set_reducer(const R& reducer)
{
    m_reducer = reducer;
}



}
