#pragma once

#include <fstream>
#include <string>
#include <filesystem>

#include "mapreduce.hpp"

/// \brief Mapper and reducer for finding shortest prefix, that uniquely
///        identifies the string in the text
namespace mapreduce_prefix
{

using pair_t = mapreduce::Framework::pair_t;
using pairs_t = mapreduce::Framework::pairs_t;
using blocks_of_pairs_t = mapreduce::Framework::blocks_of_pairs_t;

void mapper(const std::filesystem::path& fpath, const mapreduce::Block& block, pairs_t& out);
void reducer(const pairs_t& in, pairs_t& out);

}
