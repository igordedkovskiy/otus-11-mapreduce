// homework #11: mapreduce prefix

#include <iostream>
#include <type_traits>
#include <cassert>

#include "input.hpp"
#include "prefix.hpp"
//#include "words_count.hpp"

int main(int argc, const char* argv[])
{
    std::locale::global(std::locale(""));
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

    try
    {
        const auto args{read_cmd_line_args(argc, argv)};
        std::cout << args.size() << std::endl;
        if(args.count("help"))
        {
            std::cout << "Usage: options_description [options]\n";
//            boost::program_options::   show_help(args);
//            std::cout <<
            return 0;
        }

        const auto& fname{args["input-file"].as<std::string>()};
        const auto& num_of_mappers{args["num-of-mappers"].as<std::size_t>()};
        const auto& num_of_reducers{args["num-of-reducers"].as<std::size_t>()};
        std::cout << fname << ' ' << num_of_mappers << ' ' << num_of_reducers << std::endl;

        //const std::filesystem::path input(argv[1]);
        const std::filesystem::path input(fname);
        const std::filesystem::path output("out");

        mapreduce::Framework mr{mapreduce_prefix::mapper, num_of_mappers, mapreduce_prefix::reducer, num_of_reducers};
        mr.run(input, output);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
    return 0;
}

//int main()
//{
////    std::filesystem::path input("../tests/prefix-input.txt");
////    std::filesystem::path output("../tests/prefix-out.txt");
////    mapreduce::Framework mr{mapreduce_prefix::mapper, 20, mapreduce_prefix::reducer, 8};
////    mr.run(input, output);
////    return 0;

//    std::filesystem::path input("../tests/wcount-input.txt");
//    std::filesystem::path output("../tests/wcount-out.txt");
//    mapreduce::Framework mr{mapreduce_words_count::mapper, 4, mapreduce_words_count::reducer, 2};
//    mr.run(input, output);
//    return 0;
//}
