#include <fstream>
#include <string>
#include <filesystem>

#include "mapreduce.hpp"

//using mapreduce::Framework::KeyT;
using pair_t = mapreduce::Framework::pair_t;
using pairs_t = mapreduce::Framework::pairs_t;
using blocks_of_pairs_t = mapreduce::Framework::blocks_of_pairs_t;

int main()
{
    std::filesystem::path input("../tests/input.txt");
    std::filesystem::path output("../tests/out.txt");
    constexpr std::size_t num_of_mappers = 3;
    constexpr std::size_t num_of_reducers = 2;

    // TODO: read a chunk of file bounded with block
    auto mapper = [](const std::filesystem::path& fpath, [[maybe_unused]] const mapreduce::Block& block, pairs_t& out)
    {
        auto common_prefix = [](std::string& s1, std::string& s2)
        {
            if(s1[0] != s2[0])
                return std::string{};
            for(std::size_t cntr{1}; cntr < s1.size() && cntr < s2.size(); ++cntr)
            {
                if(s1[cntr] != s2[cntr])
                    return s1.substr(cntr - 1);
            }
            return s1.size() <= s2.size() ? s1 : s2;
        };

        std::fstream file{fpath.filename(), std::ios::in};

        std::string s1;
        std::getline(file, s1);
        std::string longest_common_prefix;
        for(std::string s2; std::getline(file, s2);)
        {
            auto prefix = common_prefix(s1, s2);
            if(prefix.size() > longest_common_prefix.size())
                longest_common_prefix = std::move(prefix);
            s1 = std::move(s2);
        }
        out.emplace_back(std::make_pair(std::move(longest_common_prefix), longest_common_prefix.size()));
    };

    auto reducer = [](const pairs_t& in, pairs_t& out)
    {
        auto cmp = [](const pair_t& l, const pair_t& r)
        {
            return l.second < r.second;
        };
        out.emplace_back(*std::max_element(std::begin(in), std::end(in), cmp));
    };

    mapreduce::Framework mr{mapper, num_of_mappers, reducer, num_of_reducers};
    mr.run(input, output);

    return 0;
}
