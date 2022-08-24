#include <iostream>
#include <algorithm>
#include "mapreduce.hpp"

using namespace mapreduce;

// Создаём mappers_count потоков
// В каждом потоке читаем свой блок данных
// Применяем к строкам данных функцию mapper
// Сортируем результат каждого потока
// Результат сохраняется в файловую систему (представляем, что это большие данные)
// Каждый поток сохраняет результат в свой файл (представляем, что потоки выполняются на разных узлах)
//
// Создаём reducers_count потоков
// В каждом потоке читаем свой файл (выход предыдущей фазы)
// Применяем к строкам функцию reducer
// Результат сохраняется в файловую систему
//             (во многих задачах выход редьюсера - большие данные, хотя в нашей задаче можно написать функцию reduce так, чтобы выход не был большим)
//
// Создаём reducers_count новых файлов
// Из mappers_count файлов читаем данные (результат фазы map) и перекладываем в reducers_count (вход фазы reduce)
// Перекладываем так, чтобы:
//     * данные были отсортированы
//     * одинаковые ключи оказывались в одном файле, чтобы одинаковые ключи попали на один редьюсер
//     * файлы примерно одинакового размера, чтобы редьюсеры были загружены примерно равномерно
//
// Гуглить: алгоритмы во внешней памяти, external sorting, многопутевое слияние
//
// Для упрощения задачи делаем это в один поток
// Но все данные в память одновременно не загружаем, читаем построчно и пишем
//
// Задание творческое!
// Я не уверен, что все вышеперечисленные требования выполнимы одновременно
// Возможно, придётся идти на компромисс, упрощая какие-то детали реализации
// Но это то, к чему нужно стремиться
// Проектирование ПО часто требует идти на компромиссы
// Это как оптимизация функции многих переменных с доп. ограничениями
void mapreduce::Framework::run(const std::filesystem::path& input, const std::filesystem::path& output)
{
    auto blocks {split_input(input)};
    auto mapped {map(input, blocks)};
    blocks.clear();
    auto shuffled {shuffle(mapped)};
    mapped.clear();
    auto result {reduce(shuffled)};
    shuffled.clear();
    // write into output
    std::fstream file {output.string(), std::ios::out};
    for(const auto& el:result)
        file << el.first << ' ' << el.second << '\n';
}

Framework::input_blocks_t Framework::split_input(const std::filesystem::path& file_path)
{
    const auto fsize{std::filesystem::file_size(file_path)};
    const auto block_size{(fsize % m_num_of_mappers ?
                           fsize / m_num_of_mappers :
                           fsize / m_num_of_mappers + 1)};
    input_blocks_t blocks;
    blocks.reserve(m_num_of_mappers);
    std::fstream file{file_path.string(), std::ios::in};
    std::string line;
    for(std::size_t start = file.tellg(); start < fsize; start = file.tellg())
    {
        if(start + block_size >= fsize)
        {
            blocks.emplace_back(Block{start, fsize});
            break;
        }
        file.seekg(start + block_size);
        std::getline(file, line, '\n');
        if(file.tellg() < 0)
        {
            blocks.emplace_back(Block{start, fsize});
            break;
        }
        else
            blocks.emplace_back(Block{start, static_cast<std::size_t>(file.tellg())});
    }
    return blocks;
}

Framework::blocks_of_pairs_t Framework::map(const std::filesystem::path& fpath, const input_blocks_t& blocks)
{
    std::vector<std::thread> mappers;
    std::size_t nmappers = m_num_of_mappers;//(m_num_of_mappers == blocks.size()) ? m_num_of_mappers : blocks.size();
    mappers.reserve(nmappers);
    blocks_of_pairs_t result(nmappers, pairs_t{});
    for(std::size_t cntr{0}; cntr < nmappers; ++cntr)
        mappers.emplace_back(std::thread{m_mapper,
                                         std::ref(fpath),
                                         std::ref(blocks[cntr]),
                                         std::ref(result[cntr])});
    for(auto& mapper:mappers)
        mapper.join();
    return result;
}

Framework::blocks_of_pairs_t Framework::shuffle(blocks_of_pairs_t& mapped)
{
    // shuffle. Keep sorted
    blocks_of_pairs_t shuffled{m_num_of_reducers, blocks_of_pairs_t::value_type{}};
    shuffled.reserve(m_num_of_reducers);

    auto find = [&shuffled](const KeyT& key)
    {
        for(auto it {std::begin(shuffled)}; it != std::end(shuffled); ++it)
        {
            auto el {it->find(key)};
            if(el != std::end(*it))
                return it;
        }
        auto el {std::begin(shuffled)};
        for(auto it {std::begin(shuffled)}; it != std::end(shuffled); ++it)
        {
            if(it->size() < el->size())
                el = it;
        }
        return el;
    };

    for(auto& mapped_block:mapped)
    {
        while(!mapped_block.empty())
        {
            auto first{std::begin(mapped_block)};
            auto range {mapped_block.equal_range(first->first)};
            auto where {find(first->first)};
            where->insert(range.first, range.second);
            mapped_block.erase(range.first, range.second);
        }
    }
    return shuffled;
}

Framework::pairs_t Framework::reduce(const blocks_of_pairs_t& shuffled)
{
    std::vector<std::thread> reducers;
    reducers.reserve(m_num_of_reducers);
    blocks_of_pairs_t im_result(m_num_of_reducers, pairs_t{});
    for(std::size_t cntr{0}; cntr < m_num_of_reducers; ++cntr)
        reducers.emplace_back(std::thread{m_reducer,
                                          std::ref(shuffled[cntr]),
                                          std::ref(im_result[cntr])});
    for(auto& reducer:reducers)
        reducer.join();

    // merge. Merged container must be sorted
    auto merged_block{std::begin(im_result)};
    for(auto reduced_block{std::next(merged_block)}; reduced_block != std::end(im_result); ++reduced_block)
        merged_block->merge(std::move(*reduced_block));

    pairs_t result{};
    m_reducer(*merged_block, result);
    return result;
}

Framework::exception::exception(const std::string& m):
    m_message{m}
{}

Framework::exception::exception(std::string&& m) noexcept:
    m_message{std::move(m)}
{}

const char * Framework::exception::what() const noexcept
{
    return m_message.c_str();
}
