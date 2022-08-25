#include <cassert>
//#include <boost/algorithm/string/split.hpp>
//#include <boost/algorithm/string.hpp>
#include "words_count.hpp"

namespace mapreduce_words_count
{

void mapper(const std::filesystem::path &fpath, const mapreduce::Block &block, mapreduce::Framework::pairs_t &out)
{
    std::fstream file{fpath.string(), std::ios::in | std::ios::binary};
    file.seekg(block.m_start);
    std::string word;
    while(file.tellg() >= 0 && static_cast<std::size_t>(file.tellg()) < block.m_end)
    {
        std::getline(file, word);
        std::vector<std::string> words;
//        const char* delim = " ";
//        boost::algorithm::split(words, std::move(word), boost::algorithm::is_any_of(delim), boost::token_compress_on);
//        for(auto& word:words)
        std::stringstream s{std::move(word)};
        while(s >> word)
        {
//            if(word.back() == '\r')
//                word.resize(word.size() - 1);
            for(auto& c:word)
                c = std::tolower(c);

            std::string alpha;
            std::copy_if(word.begin(), word.end(), std::back_inserter(alpha),
                         [](unsigned char c) { return std::isalpha(c); });
            word.swap(alpha);

            auto el = out.find(word);
            if(el != std::end(out))
                ++el->second;
            else
                out.insert(std::make_pair(std::move(word), 1));
        }
    }
}

void reducer(const mapreduce::Framework::pairs_t &in, mapreduce::Framework::pairs_t &out)
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
}

void classical()
{
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
