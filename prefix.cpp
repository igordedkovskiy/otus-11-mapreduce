#include "prefix.hpp"

namespace mapreduce_prefix
{

void mapper(const std::filesystem::path &fpath, const mapreduce::Block &block, pairs_t &out)
{
    auto common_prefix = [](const std::string& s1, const std::string& s2)
    {
        if(s1[0] != s2[0])
            return std::string{};
        for(std::size_t cntr{1}; cntr < s1.size() && cntr < s2.size(); ++cntr)
        {
            if(s1[cntr] != s2[cntr])
                return s1.substr(0, cntr);
        }
        return (s1.size() <= s2.size()) ? s1 : s2;
    };

    std::fstream file{fpath.string(), std::ios::in | std::ios::binary};
    file.seekg(block.m_start);
    std::string s1, s2, longest_common_prefix;
    std::getline(file, s1);
    longest_common_prefix = s1[0];
    while(file.tellg() >= 0 && static_cast<std::size_t>(file.tellg()) < block.m_end)
    {
        std::getline(file, s2);
        auto prefix {common_prefix(s1, s2)};
        if(prefix.size() > longest_common_prefix.size())
            longest_common_prefix = std::move(prefix);
        s1 = std::move(s2);
    }
    out.insert(std::make_pair(std::move(longest_common_prefix), longest_common_prefix.size()));
}

void reducer(const pairs_t &in, pairs_t &out)
{
    auto cmp = [](const pair_t& l, const pair_t& r)
    {
        return l.second < r.second;
    };
    out.insert(*std::max_element(std::begin(in), std::end(in), cmp));
}

}
