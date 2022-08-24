#include <vector>
#include <map>
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
    std::filesystem::path input("../tests/prefix-input.txt");
    std::filesystem::path output("../tests/prefix-out.txt");

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
    {
        mapreduce::Framework mr{mapreduce_prefix::mapper, 20, mapreduce_prefix::reducer, 8};
        mr.run(input, output);
        check();
    }
}


namespace word_count
{

void classical()
{
//    std::cout << "Classical word count" << std::endl;

    std::filesystem::path input("../tests/wcount-input.txt");
    std::fstream file{input.string(), std::ios::in};
    std::string s;
    std::map<std::string, int> result;
    while(file >> s)
    {
        for(auto& c:s)
            c = std::tolower(c);
        std::string alpha;
        std::copy_if(s.begin(), s.end(), std::back_inserter(alpha), [](unsigned char c) {return std::isalpha(c) || std::isspace(c); });
        s.swap(alpha);

        auto el = result.find(s);
        if(el != std::end(result))
            ++el->second;
        else
            result.insert(std::make_pair(std::move(s), 1));
    }

    {
        std::filesystem::path output("../tests/wcount-classical-out.txt");
        std::fstream file{output.string(), std::ios::out};
        for(auto& [word, count]: result)
            file << word << " " << count << std::endl;
    }
}

}


TEST(TEST_PREFIX, mr_word_count)
{
    std::filesystem::path input("../tests/wcount-input.txt");
    std::filesystem::path output("../tests/wcount-out.txt");

    auto check = [&output]()
    {
        std::filesystem::path input_ref("../tests/wcount-classical-out.txt");
        std::fstream file_ref{input_ref.string(), std::ios::in};
        std::fstream file{output.string(), std::ios::in};
        EXPECT_EQ(std::filesystem::file_size(output), std::filesystem::file_size(input_ref));

        std::string ref;
        for(std::string s; std::getline(file_ref, s);)
            ref += std::move(s);

        std::string res;
        for(std::string s; std::getline(file, s);)
            res += std::move(s);

        EXPECT_EQ(res, ref);
    };

    auto mapper = [](const std::filesystem::path &fpath, const mapreduce::Block &block, mapreduce::Framework::pairs_t &out)
    {
        std::fstream file{fpath.string(), std::ios::in};
        file.seekg(block.m_start);
        std::string s;
        do
        {
            file >> s;

            for(auto& c:s)
                c = std::tolower(c);

            std::string alpha;
            std::copy_if(s.begin(), s.end(), std::back_inserter(alpha), [](unsigned char c) {return std::isalpha(c) || std::isspace(c); });
            s.swap(alpha);

            auto el = out.find(s);
            if(el != std::end(out))
                ++el->second;
            else
                out.insert(std::make_pair(std::move(s), 1));
        }
        while(block.m_end >= static_cast<decltype(block.m_end)>(file.tellg()));
    };
    auto reducer = [](const mapreduce::Framework::pairs_t &in, mapreduce::Framework::pairs_t &out)
    {
        for(auto it {std::begin(in)}; it != std::end(in);)
        {
            auto range {in.equal_range(it->first)};
            std::size_t count{0};
            for(auto it2{range.first}; it2 != range.second; ++it2)
                count += it2->second;
            out.insert(std::make_pair(std::move(range.first->first), count));
            it = range.second;
        }
    };

    word_count::classical();
    mapreduce::Framework mr{mapper, 5, reducer, 3};
    mr.run(input, output);
    check();
}


int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
