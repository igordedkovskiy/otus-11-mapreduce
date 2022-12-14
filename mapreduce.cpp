#include <iostream>
#include <algorithm>
#include "mapreduce.hpp"

namespace mapreduce
{

void mapreduce::Framework::run(const std::filesystem::path& input, const std::filesystem::path& output)
{
    auto blocks {split_input(input)};
    auto mapped {map(input, blocks)};
    blocks.clear();
    auto shuffled {shuffle(mapped)};
    mapped.clear();
    auto result {reduce(shuffled)};
    shuffled.clear();
    // write into output
    std::fstream file {output.string(), std::ios::out};
    for(const auto& el:result)
        file << el.first << ' ' << el.second << '\n';
}

Framework::input_blocks_t Framework::split_input(const std::filesystem::path& file_path)
{
    const auto fsize{std::filesystem::file_size(file_path)};
    const auto block_size{(fsize % m_num_of_mappers ?
                           fsize / m_num_of_mappers + 1 :
                           fsize / m_num_of_mappers)};
    input_blocks_t blocks;
    blocks.reserve(m_num_of_mappers);
    std::fstream file{file_path.string(), std::ios::in | std::ios::binary};
    for(std::size_t start{0}; start + block_size - 1 < fsize;)
    {
        const auto cur {start + block_size - 1};
        if(cur >= fsize)
        {
            blocks.emplace_back(Block{start, fsize});
            break;
        }
        std::string s;
        file.seekg(cur);
        std::getline(file, s);
        std::size_t end {(file.tellg() < 0) ? fsize : static_cast<decltype(end)>(file.tellg())};
        blocks.emplace_back(Block{start, end});
        start = end;
    }
    if(blocks.back().m_end < fsize)
    {
        if(blocks.size() < m_num_of_mappers)
            blocks.emplace_back(Block{blocks.back().m_end, fsize});
        else
            blocks.back().m_end = fsize;
    }
    return blocks;
}

Framework::blocks_of_pairs_t Framework::map(const std::filesystem::path& fpath, const input_blocks_t& blocks)
{
    const std::size_t nmappers {(m_num_of_mappers == blocks.size()) ? m_num_of_mappers : blocks.size()};
    std::vector<std::thread> mappers;
    mappers.reserve(nmappers);
    blocks_of_pairs_t result(nmappers, pairs_t{});
    for(std::size_t cntr{0}; cntr < nmappers; ++cntr)
        mappers.emplace_back(std::thread{m_mapper,
                                         std::ref(fpath),
                                         std::ref(blocks[cntr]),
                                         std::ref(result[cntr])});
    for(auto& mapper:mappers)
        mapper.join();
    return result;
}

Framework::blocks_of_pairs_t Framework::shuffle(blocks_of_pairs_t& mapped)
{
    // shuffle. Keep sorted
    const std::size_t nreducers {(m_num_of_reducers < mapped.size()) ? m_num_of_reducers : mapped.size()};
    blocks_of_pairs_t shuffled{nreducers, blocks_of_pairs_t::value_type{}};
    shuffled.reserve(nreducers);

    auto find = [&shuffled](const KeyT& key)
    {
        for(auto it {std::begin(shuffled)}; it != std::end(shuffled); ++it)
        {
            auto el {it->find(key)};
            if(el != std::end(*it))
                return it;
        }
        auto el {std::begin(shuffled)};
        for(auto it {std::begin(shuffled)}; it != std::end(shuffled); ++it)
        {
            if(it->size() < el->size())
                el = it;
        }
        return el;
    };

    for(auto& mapped_block:mapped)
    {
        while(!mapped_block.empty())
        {
            auto first{std::begin(mapped_block)};
            auto range {mapped_block.equal_range(first->first)};
            auto where {find(first->first)};
            where->insert(range.first, range.second);
            mapped_block.erase(range.first, range.second);
        }
    }
    return shuffled;
}

Framework::pairs_t Framework::reduce(const blocks_of_pairs_t& shuffled)
{
    const std::size_t nreducers {(m_num_of_reducers == shuffled.size()) ? m_num_of_reducers : shuffled.size()};
    std::vector<std::thread> reducers;
    reducers.reserve(nreducers);
    blocks_of_pairs_t im_result(nreducers, pairs_t{});
    for(std::size_t cntr{0}; cntr < nreducers; ++cntr)
        reducers.emplace_back(std::thread{m_reducer,
                                          std::ref(shuffled[cntr]),
                                          std::ref(im_result[cntr])});
    for(auto& reducer:reducers)
        reducer.join();

    // merge. Merged container must be sorted
    auto merged_block{std::begin(im_result)};
    for(auto reduced_block{std::next(merged_block)}; reduced_block != std::end(im_result); ++reduced_block)
        merged_block->merge(std::move(*reduced_block));

    pairs_t result{};
    m_reducer(*merged_block, result);
    return result;
}

}
