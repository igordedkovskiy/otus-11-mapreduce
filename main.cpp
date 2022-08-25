// homework #11 mapreduce

#include <iostream>
#include <type_traits>
#include <cassert>

#include "prefix.hpp"
#include "words_count.hpp"

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
//    std::filesystem::path input("../tests/prefix-input.txt");
//    std::filesystem::path output("../tests/prefix-out.txt");
//    mapreduce::Framework mr{mapreduce_prefix::mapper, 20, mapreduce_prefix::reducer, 8};
//    mr.run(input, output);
//    return 0;

    std::filesystem::path input("../tests/wcount-input.txt");
    std::filesystem::path output("../tests/wcount-out.txt");
    mapreduce::Framework mr{mapreduce_words_count::mapper, 4, mapreduce_words_count::reducer, 2};
    mr.run(input, output);
    return 0;

//    std::filesystem::path input("../tests/wcount-input.txt");
//    const auto fsize{std::filesystem::file_size(input)};
//    std::fstream file{input.string(), std::ios::in | std::ios::binary};
//    std::cout << fsize << '\n';
//    for(std::size_t cntr = 0; cntr < fsize; ++cntr)
//    {
//        file.seekg(cntr);
//        std::cout << cntr << ' ' << file.tellg();
//        char c;
//        file.get(c);
//        std::cout << ' ' << c << std::endl;
//    }

//    char c;
//    file.seekg(0);
//    file.get(c);
//    assert(c == 'A');
//    file.seekg(7);
//    file.get(c);
//    assert(c == 'd');
//    file.seekg(78);
//    file.get(c);
//    assert(c == 'a');
//    file.seekg(79);
//    file.get(c);
//    assert(c == 'n');
//    file.seekg(80);
//    file.get(c);
//    assert(c == 'd');

//    std::size_t m_num_of_mappers = 4;
//    const auto block_size{(fsize % m_num_of_mappers ?
//                           fsize / m_num_of_mappers :
//                           fsize / m_num_of_mappers + 1)};
//    using mapreduce::Block;
//    using input_blocks_t = std::vector<Block>;
//    input_blocks_t blocks;
//    blocks.reserve(m_num_of_mappers);
//    file.seekg(0);
//    for(std::size_t start{0}; start + block_size < fsize;)
//    {
//        const auto cur {start + block_size - 1};
//        if(cur >= fsize)
//        {
//            blocks.emplace_back(Block{start, fsize});
//            break;
//        }
//        std::string s;
//        file.seekg(cur);
//        std::getline(file, s);
//        std::cout << s << std::endl;

//        std::size_t end = (file.tellg() < 0) ? fsize : static_cast<decltype(end)>(file.tellg());
//        file.seekg(file.tellg());
//        std::getline(file, s);
//        std::cout << s << std::endl << std::endl;
//        blocks.emplace_back(Block{start, end});
//        start = end;
//    }
//    if(blocks.back().m_end < fsize)
//    {
//        if(blocks.size() < m_num_of_mappers)
//            blocks.emplace_back(Block{blocks.back().m_end, fsize});
//        else
//            blocks.back().m_end = fsize;
//    }

//    for(const auto& [start, end]:blocks)
//        std::cout << start << ' ' << end << std::endl;

//    {
//        file.close();
//        file.open(input.string(), std::ios::in | std::ios::binary);
//        for(const auto& [start, end]:blocks)
//        {
//            file.seekg(start);
//            std::string s;
//            do
//            {
//                std::getline(file, s);
////                if(!s.empty())
//                std::cout << s << std::endl;
//            }
//            while(file.tellg() >= 0 && static_cast<std::size_t>(file.tellg()) < end);
////            std::cout << std::endl;
//        }
//    }


//    {
//        char c = '!';
//        file.close();
//        file.open(input.string(), std::ios::in | std::ios::binary);
//        file.seekg(blocks[0].m_start);
//        file.get(c);
//        std::cout << c << std::endl;
//        assert(c == 'A');
//        file.seekg(blocks[0].m_end);
//        file.get(c);
//        std::cout << c << std::endl;
//        assert(c == 'e');

//        file.seekg(blocks[1].m_start);
//        file.get(c);
//        assert(c == 'e');
//        file.seekg(blocks[1].m_end);
//        file.get(c);
//        assert(c == 't');

//        file.seekg(blocks[2].m_start);
//        file.get(c);
//        assert(c == 't');
//        file.seekg(blocks[2].m_end);
//        file.get(c);
//        assert(c == 'v');

//        file.seekg(blocks[3].m_start);
//        file.get(c);
//        assert(c == 'v');
//    }

//    return 0;
}
