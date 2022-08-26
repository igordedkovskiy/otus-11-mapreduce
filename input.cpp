#include <iostream>
#include <vector>
#include <string>
#include <filesystem>
#include <thread>

#include "input.hpp"

input_t parse_cmd_line(int argc, const char *argv[])
{
    namespace fs = std::filesystem;
    namespace po = boost::program_options;

    po::options_description desc{"Options"};

    auto in = [](std::size_t min, std::size_t max, char const * const opt)
    {
        auto ret = [opt, min, max](unsigned short v)
        {
            if(v < min || v > max)
            {
                throw po::validation_error(po::validation_error::invalid_option_value, opt, std::to_string(v));
            }
        };
        return ret;
    };

    const auto threads_limit {4 * std::thread::hardware_concurrency()};

    desc.add_options()
            ("help,h", "Help page")

            ("input-file,s", po::value<std::string>()->required(),
             "input file")

            ("num-of-mappers,m", po::value<std::size_t>()->required()
             ->notifier(in(1, threads_limit, "num-of-mappers")),
             "number of mappers")

            ("num-of-reducers,r", po::value<std::size_t>()->required()
             ->notifier(in(1, threads_limit, "num-of-reducers")),
             "number of reducers")
            ;

    po::positional_options_description positionalDescription;
    positionalDescription.add("input-file", 1);
    positionalDescription.add("num-of-mappers", 1);
    positionalDescription.add("num-of-reducers", 1);

    po::variables_map vm;
//    po::store(parse_command_line(argc, argv, desc) ,vm);
    po::store(po::command_line_parser(argc, argv)
              .options( desc )
              .positional( positionalDescription )
              .run()
              ,
              vm);

    if(vm.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " <input-file> <num-of-mappers> <num-of-reducers>\n";
        std::cout << "num-of-mappers  = [" << 1 << ';' << threads_limit << "]\n";
        std::cout << "num-of-reducers = [" << 1 << ';' << threads_limit << "]\n";
        std::cout << "num-of-mappers >=  num-of-reducers\n\n";
        std::cout << desc;
        return std::make_pair(vm, false);
    }

    if(vm["num-of-reducers"].as<std::size_t>() > vm["num-of-mappers"].as<std::size_t>())
    {
        std::cout << "Contraint violated: ";
        std::cout << "num-of-mappers >=  num-of-reducers\n";
        return std::make_pair(vm, false);
    }

    po::notify(vm);

    return std::make_pair(vm, true);
}
