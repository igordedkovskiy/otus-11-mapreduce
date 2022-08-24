#include <vector>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <cassert>
#include <numeric>
#include <iterator>
#include "gtest/gtest.h"

#include "prefix.hpp"

using pair_t = mapreduce::Framework::pair_t;
using pairs_t = mapreduce::Framework::pairs_t;
using blocks_of_pairs_t = mapreduce::Framework::blocks_of_pairs_t;

TEST(TEST_PREFIX, mr_prefix)
{
    std::filesystem::path input("../tests/input-prefix.txt");
    std::filesystem::path output("../tests/out-prefix.txt");

    auto check = [&output]()
    {
        std::fstream file{output.string(), std::ios::in};
        std::string prefix;
        std::getline(file, prefix, ';');
        std::cout << "prefix: " << prefix << std::endl;
        std::string length;
        std::getline(file, length, '\n');
        ASSERT_TRUE(prefix == "anabanbanana" || prefix == "bananabanana");
        EXPECT_EQ(length, "12");
    };

    {
        mapreduce::Framework mr{mapreduce_prefix::mapper, 3, mapreduce_prefix::reducer, 2};
        mr.run(input, output);
        check();
    }
    {
        mapreduce::Framework mr{mapreduce_prefix::mapper, 5, mapreduce_prefix::reducer, 3};
        mr.run(input, output);
        check();
    }
    {
        mapreduce::Framework mr{mapreduce_prefix::mapper, 10, mapreduce_prefix::reducer, 3};
        mr.run(input, output);
        check();
    }
    {
        mapreduce::Framework mr{mapreduce_prefix::mapper, 1, mapreduce_prefix::reducer, 1};
        mr.run(input, output);
        check();
    }
    {
        mapreduce::Framework mr{mapreduce_prefix::mapper, 1, mapreduce_prefix::reducer, 2};
        mr.run(input, output);
        check();
    }
    {
        mapreduce::Framework mr{mapreduce_prefix::mapper, 5, mapreduce_prefix::reducer, 8};
        mr.run(input, output);
        check();
    }
//    {
//        mapreduce::Framework mr{mapreduce_prefix::mapper, 20, mapreduce_prefix::reducer, 8};
//        mr.run(input, output);
//        check();
//    }
}


namespace word_count
{
std::string input("A MapReduce program is composed of a map procedure, which performs filtering and sorting (such as sorting students by first name into queues, one queue for each name), and a reduce method, which performs a summary operation (such as counting the number of students in each queue, yielding name frequencies). The MapReduce System (also called infrastructure or framework) orchestrates the processing by marshalling the distributed servers, running the various tasks in parallel, managing all communications and data transfers between the various parts of the system, and providing for redundancy and fault tolerance. ");

std::string prepare(const std::string& input)
{
    std::string lower;
    std::transform(input.begin(), input.end(), std::back_inserter(lower), [](unsigned char c) {return std::tolower(c); });
    std::string alpha_lower;
    std::copy_if(lower.begin(), lower.end(), std::back_inserter(alpha_lower), [](unsigned char c) {return std::isalpha(c) || std::isspace(c); });
    return alpha_lower;
}

std::vector<std::string> split(const std::string& input)
{
    std::istringstream iss(input);
    return { std::istream_iterator<std::string>(iss), {} };
}


void classical()
{
//    std::cout << "Classical word count" << std::endl;

    std::filesystem::path input("../tests/input-wcount.txt");
    std::fstream file{input.string(), std::ios::in};
    std::string s, in_str;
    while(std::getline(file, s))
        in_str += std::move(s);
    auto words = split(prepare(in_str));

    std::map<std::string, int> result;
    for(const auto& word: words)
    {
        if(result.find(word) == result.end())
            result[word] = 0;
        result[word] += 1;
    }

    {
        std::filesystem::path output("../tests/output-wcount-classical.txt");
        std::fstream file{output.string(), std::ios::out};
        for(auto [word, count]: result)
            file << word << " " << count << std::endl;
    }
}

}


//TEST(TEST_PREFIX, mr_word_count)
//{
//    std::filesystem::path input("../tests/input-wcount.txt");
//    std::filesystem::path output("../tests/out-wcount.txt");

//    auto check = [&output]()
//    {
//        std::filesystem::path input_ref("../tests/output-wcount-classical.txt");
//        std::fstream file_ref{input_ref.string(), std::ios::in};
//        std::fstream file{output.string(), std::ios::in};
//        EXPECT_EQ(std::filesystem::file_size(output), std::filesystem::file_size(input_ref));

//        std::string ref;
//        for(std::string s; std::getline(file_ref, s);)
//            ref += std::move(s);

//        std::string res;
//        for(std::string s; std::getline(file, s);)
//            res += std::move(s);

//        EXPECT_EQ(res, ref);
//    };

//    auto mapper = [](const std::filesystem::path &fpath, const mapreduce::Block &block, pairs_t &out)
//    {
//        std::fstream file{fpath.string(), std::ios::in};
//        file.seekg(block.m_start);
//        std::string s;
//        do
//        {
//            file >> s;
//            out.emplace_back(std::make_pair(std::move(s), 1));
//        }
//        while(block.m_end >= static_cast<decltype(block.m_end)>(file.tellg()));
//    };

//    auto reducer = [](const pairs_t &in, pairs_t &out)
//    {
////        auto cmp = [](const auto& l, const auto& r)
////        {
////            return std::lexicographical_compare(std::begin(l.first), std::end(l.first),
////                                                std::begin(r.first), std::end(r.first));
////        };

//        for(auto first{std::begin(in)}; first != std::end(in);)
//        {
////            auto last{std::upper_bound(first, std::end(in), *first, cmp)};
////            out.emplace_back(std::make_pair(first->first, std::distance(first, last)));
////            first = last;

//            std::size_t cntr{0};
//            auto last {first};
//            while(last != std::end(in) && last->first == first->first)
//            {
//                ++last;
//                ++cntr;
//            }
//            out.emplace_back(std::make_pair(first->first, cntr));
//            first = last;
//        }
//    };
////    ASSERT_TRUE(true);
////    return;

//    word_count::classical();
//    mapreduce::Framework mr{mapper, 5, reducer, 3};
//    mr.run(input, output);
//    check();
////    ASSERT_TRUE(true);
//}


int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
