#pragma once

#include <boost/program_options.hpp>

using input_t = boost::program_options::variables_map;

input_t read_cmd_line_args(int argc, const char *argv[]);
