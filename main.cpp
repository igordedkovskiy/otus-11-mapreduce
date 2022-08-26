// homework #11: mapreduce prefix

#include <iostream>
#include <type_traits>
#include <cassert>

//#include "input.hpp"
#include "prefix.hpp"
//#include "words_count.hpp"

int main(int argc, const char* argv[])
{
    std::locale::global(std::locale(""));
    try
    {
        if(argc != 4)
        {
            std::cout << "Usage: " << argv[0] << " <input-file> <num-of-mappers> <num-of-reducers>: "
                         "<num-of-mappers> >= <num-of-reducers>" << std::endl;
            return 0;
        }

        const auto nmappers {std::stoi(std::string{argv[2]})};
        const auto nreducers {std::stoi(std::string{argv[3]})};
        if(nmappers < 1 || nreducers < 1)
        {
            std::cout << "Usage: " << argv[0] << " <input-file> <num-of-mappers> <num-of-reducers>: "
                         "\nNumber of reducers and number of mappers must be greater than 1"
                      << std::endl;
            return 0;
        }

        const std::size_t num_of_mappers {static_cast<std::remove_cv_t<decltype(num_of_mappers)>>(nmappers)};
        const std::size_t num_of_reducers {static_cast<std::remove_cv_t<decltype(num_of_reducers)>>(nreducers)};
        const std::filesystem::path input(argv[1]);


//        auto [opts, res] {parse_cmd_line(argc, argv)};
//        if(!res)
//            return 0;
//        const auto& fname {opts["input-file"].as<std::string>()};
//        const auto& num_of_mappers {opts["num-of-mappers"].as<std::size_t>()};
//        const auto& num_of_reducers {opts["num-of-reducers"].as<std::size_t>()};

//        const std::filesystem::path input(fname);

        if(num_of_reducers > num_of_mappers)
        {
            std::cout << "Number of reducers must be equal or less than number of mappers."
                      << " Run again" << std::endl;
            return 0;
        }

        if(!std::filesystem::exists(input))
        {
            std::cout << "File " << input << " doesn't exist" << std::endl;
            return 0;
        }
        const std::filesystem::path output("out");

        mapreduce::Framework mr{mapreduce_prefix::mapper, num_of_mappers,
                                mapreduce_prefix::reducer, num_of_reducers};
        mr.run(input, output);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
    return 0;
}
