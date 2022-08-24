// homework #11 mapreduce

#include <iostream>
#include <type_traits>

#include "prefix.hpp"

//int main(int argc, char* argv[])
//{
//    std::locale::global(std::locale(""));
//    if(argc != 4)
//    {
//        std::cerr << "Usage: " << argv[0] << " <src> <mnum> <rnum>: <mnum> >= <rnum>" << std::endl;
//        return 1;
//    }

//    const auto nmappers {std::stoi(std::string{argv[2]})};
//    const auto nreducers {std::stoi(std::string{argv[3]})};
//    if(nmappers < 1 || nreducers < 1)
//    {
//        std::cerr << "Usage: " << argv[0] << " <src> <mnum> <rnum>: <mnum> > 0, <rnum> > 0"
//                     "\nNumber of reducers and number of mappers must be greater than 1"
//                  << std::endl;
//        return 1;
//    }

//    const std::size_t num_of_mappers {static_cast<std::remove_cv_t<decltype(num_of_mappers)>>(nmappers)};
//    const std::size_t num_of_reducers {static_cast<std::remove_cv_t<decltype(num_of_reducers)>>(nreducers)};
//    if(num_of_reducers > num_of_mappers)
//    {
//        std::cerr << "Usage: " << argv[0] << " <src> <mnum> <rnum>: <mnum> >= <rnum>"
//                     "\nNumber of reducers must be equal or less than number of mappers"
//                  << std::endl;
//        return 1;
//    }

//    const std::filesystem::path input(argv[1]);
//    const std::filesystem::path output("out");

//    mapreduce::Framework mr{mapreduce_prefix::mapper, num_of_mappers, mapreduce_prefix::reducer, num_of_reducers};
//    mr.run(input, output);
//    return 0;
//}

int main()
{
//    std::filesystem::path input("../tests/input-prefix.txt");
//    std::filesystem::path output("../tests/out-prefix.txt");
//    mapreduce::Framework mr{mapreduce_prefix::mapper, 3, mapreduce_prefix::reducer, 2};
//    mr.run(input, output);

    std::filesystem::path input("../tests/wcount-input.txt");
    std::filesystem::path output("../tests/wcount-out.txt");
    auto mapper = [](const std::filesystem::path &fpath, const mapreduce::Block &block, mapreduce::Framework::pairs_t &out)
    {
        std::fstream file{fpath.string(), std::ios::in};
        file.seekg(block.m_start);

        std::string tmp(block.m_end - block.m_start, ' ');
        {
            std::fstream file{fpath.string(), std::ios::in};
            file.seekg(block.m_start);
            std::getline(file, tmp);
        }

        std::string s;
        do
        {
            file >> s;
            if(s.empty())
                continue;

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
        while(block.m_end > static_cast<decltype(block.m_end)>(file.tellg()));
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
    try
    {
        mapreduce::Framework mr{mapper, 5, reducer, 3};
        mr.run(input, output);
    }
    catch(std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
