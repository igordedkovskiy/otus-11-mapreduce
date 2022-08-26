// homework #11: mapreduce prefix

#include <iostream>
#include <type_traits>
#include <cassert>

#include "input.hpp"
#include "prefix.hpp"

int main(int argc, const char* argv[])
{
    std::locale::global(std::locale(""));
    try
    {
        auto [opts, res] {parse_cmd_line(argc, argv)};
        if(!res)
            return 0;
        const auto& fname {opts["input-file"].as<std::string>()};
        const auto& num_of_mappers {opts["num-of-mappers"].as<std::size_t>()};
        const auto& num_of_reducers {opts["num-of-reducers"].as<std::size_t>()};

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
