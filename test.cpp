#include <vector>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <cassert>
#include "gtest/gtest.h"

#include "prefix.hpp"

using pair_t = mapreduce::Framework::pair_t;
using pairs_t = mapreduce::Framework::pairs_t;
using blocks_of_pairs_t = mapreduce::Framework::blocks_of_pairs_t;

TEST(TEST_FRAMEWORK, framework)
{
    std::filesystem::path input("../tests/input-prefix.txt");
    std::filesystem::path output("../tests/out-prefix.txt");
    constexpr std::size_t num_of_mappers = 3;
    constexpr std::size_t num_of_reducers = 2;

//    auto mapper = [](const std::filesystem::path& fpath, const mapreduce::Block& block, pairs_t& out)
//    {
//        auto common_prefix = [](const std::string& s1, const std::string& s2)
//        {
//            if(s1[0] != s2[0])
//                return std::string{};
//            for(std::size_t cntr{1}; cntr < s1.size() && cntr < s2.size(); ++cntr)
//            {
//                if(s1[cntr] != s2[cntr])
//                    return s1.substr(0, cntr);
//            }
//            return (s1.size() <= s2.size()) ? s1 : s2;
//        };

//        std::fstream file{fpath.string(), std::ios::in};
//        file.seekg(block.m_start);
//        std::string s1, s2, longest_common_prefix;
//        std::getline(file, s1, '\n');
//        do
//        {
//            std::getline(file, s2, '\n');
//            auto prefix {common_prefix(s1, s2)};
//            if(prefix.size() > longest_common_prefix.size())
//                longest_common_prefix = std::move(prefix);
//            s1 = std::move(s2);
//        }
//        while(block.m_end > static_cast<decltype(block.m_end)>(file.tellg()));
//        out.emplace_back(std::make_pair(std::move(longest_common_prefix), longest_common_prefix.size()));
//    };

//    auto reducer = [](const pairs_t& in, pairs_t& out)
//    {
//        auto cmp = [](const pair_t& l, const pair_t& r)
//        {
//            return l.second < r.second;
//        };
//        out.emplace_back(*std::max_element(std::begin(in), std::end(in), cmp));
//    };

    mapreduce::Framework mr{mapreduce_prefix::mapper, num_of_mappers, mapreduce_prefix::reducer, num_of_reducers};
    mr.run(input, output);

    {
        std::fstream file{output.string(), std::ios::in};
        std::string prefix;
        std::getline(file, prefix, ';');
        std::cout << "prefix: " << prefix << std::endl;
        std::string length;
        std::getline(file, length, '\n');
        ASSERT_TRUE(prefix == "anabanbanana" || prefix == "bananabanana");
        EXPECT_EQ(length, "12");
    }
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
