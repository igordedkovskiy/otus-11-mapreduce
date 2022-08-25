#include <vector>
#include <string>
#include <filesystem>

#include "input.hpp"

input_t read_cmd_line_args(int argc, const char *argv[])
{
    namespace fs = std::filesystem;
    namespace po = boost::program_options;

    po::options_description desc{"Options"};

    desc.add_options()
            ("input-file,s", po::value<std::string>()->required(), "input file")
            ("num-of-mappers,m", po::value<std::size_t>()->required(), "number of mappers")
            ("num-of-reducers,r", po::value<std::size_t>()->required(), "number of reducers")
            ("help,h", "Help page")
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
    po::notify(vm);

//    if (vm.count("help")) {
//        std::cout << "Usage: options_description [options]\n";
//        std::cout << desc;
//    }

    return vm;
}
