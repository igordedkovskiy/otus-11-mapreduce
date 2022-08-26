// homework #11: mapreduce prefix

#include <iostream>
#include <type_traits>
#include <cassert>
#include <tuple>
#include <exception>

//#include "input.hpp"
#include "prefix.hpp"
//#include "words_count.hpp"


std::tuple<std::string, std::size_t, std::size_t> parse_cmd(int argc, const char* argv[])
{
    struct Exception: public std::exception
    {
        Exception(std::string m): m_msg{std::move(m)}{}
        const char* what() const noexcept override { return m_msg.c_str(); }
        std::string m_msg;
    };

    if(argc != 4)
    {
        std::stringstream s;
        s << "Usage: " << argv[0] << " <input-file> <num-of-mappers> <num-of-reducers>: "
                                     "<num-of-mappers> >= <num-of-reducers>";
        throw Exception{std::move(s.str())};
    }

    const auto nmappers {std::stoi(std::string{argv[2]})};
    const auto nreducers {std::stoi(std::string{argv[3]})};
    if(nmappers < 1 || nreducers < 1)
    {
        std::stringstream s;
        s << "Usage: " << argv[0] << " <input-file> <num-of-mappers> <num-of-reducers>: "
                                     "\nNumber of reducers and number of mappers must be equal or greater than 1";
        throw Exception{std::move(s.str())};
    }

    std::string fname {argv[1]};
    const std::size_t num_of_mappers {static_cast<std::remove_cv_t<decltype(num_of_mappers)>>(nmappers)};
    const std::size_t num_of_reducers {static_cast<std::remove_cv_t<decltype(num_of_reducers)>>(nreducers)};
    if(num_of_reducers > num_of_mappers)
    {
        std::stringstream s;
        s << "Usage: " << argv[0] << " <input-file> <num-of-mappers> <num-of-reducers>: "
                       "Number of reducers must be equal or less than number of mappers. Run again";
        throw Exception{std::move(s.str())};
    }
    return std::make_tuple(std::move(fname), num_of_mappers, num_of_reducers);
}


int main(int argc, const char* argv[])
{
    std::locale::global(std::locale(""));
    try
    {
//        auto [opts, res] {parse_cmd_line(argc, argv)};
//        if(!res)
//            return 0;
//        const auto& fname {opts["input-file"].as<std::string>()};
//        const auto& num_of_mappers {opts["num-of-mappers"].as<std::size_t>()};
//        const auto& num_of_reducers {opts["num-of-reducers"].as<std::size_t>()};
//        if(num_of_reducers > num_of_mappers)
//        {
//            std::cout << "Number of reducers must be equal or less than number of mappers."
//                      << " Run again" << std::endl;
//            return 0;
//        }

        auto [fname, num_of_mappers, num_of_reducers] = parse_cmd(argc, argv);

        const std::filesystem::path input(fname);
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
