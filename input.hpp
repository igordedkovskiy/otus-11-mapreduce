#pragma once

#include <utility>
#include <boost/program_options.hpp>

using input_t = std::pair<boost::program_options::variables_map, bool>;

input_t parse_cmd_line(int argc, const char *argv[]);
