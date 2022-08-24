// homework #11 mapreduce

#include <iostream>
#include <type_traits>

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
//    std::filesystem::path input("../tests/input-prefix.txt");
//    std::filesystem::path output("../tests/out-prefix.txt");
//    mapreduce::Framework mr{mapreduce_prefix::mapper, 3, mapreduce_prefix::reducer, 2};
//    mr.run(input, output);

    std::filesystem::path input("../tests/wcount-input.txt");
    std::filesystem::path output("../tests/wcount-out.txt");
    mapreduce::Framework mr{mapreduce_words_count::mapper, 4, mapreduce_words_count::reducer, 2};
    mr.run(input, output);
    return 0;
}
