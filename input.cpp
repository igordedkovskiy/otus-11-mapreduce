#include <iostream>
#include <vector>
#include <string>
#include <filesystem>

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

//    desc.add_options()
//            ("input-file,s", po::value<std::string>()->required(), "input file")
//            ("num-of-mappers,m", po::value<std::string>()->required())//->notifier(in(-5, 10, "num-of-mappers")), "number of mappers")
//            ("num-of-reducers,r", po::value<std::size_t>()->required()->notifier(in(1, 10, "num-of-reducers")), "number of reducers")
//            ("help,h", "Help page")
//            ;
    desc.add_options()
            ("help,h", "Help page")
            ("input-file,s", po::value<std::string>()->required(), "input file")

            ("num-of-mappers,m", po::value<std::size_t>()->required()
             ->notifier(in(1, std::numeric_limits<std::size_t>::max(),
                           "num-of-mappers")), "number of mappers")

            ("num-of-reducers,r", po::value<std::size_t>()->required()
             ->notifier(in(1, std::numeric_limits<std::size_t>::max(),
                           "num-of-reducers")), "number of reducers")
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
        std::cout << desc;
        return std::make_pair(vm, false);
    }
    po::notify(vm);

    return std::make_pair(vm, true);
}
