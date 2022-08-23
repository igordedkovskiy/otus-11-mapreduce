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
    auto shuffled {shuffle(mapped)};
    auto result {reduce(shuffled)};
    // write into output
    std::fstream file {output.string(), std::ios::out};
    for(const auto& el:result)
        file << '[' << el.first << ',' << el.second << "]\n";
}

// Эта функция не читает весь файл.
// Определяем размер файла в байтах.
// Делим размер на количество блоков - получаем границы блоков.
// Читаем данные только вблизи границ.
// Выравниваем границы блоков по границам строк.
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
    for(std::size_t start{0}; start < fsize;)
    {
        if(start + block_size >= fsize)
        {
            blocks.emplace_back(Block{start, fsize - 1});
            break;
        }
        file.seekg(start + block_size);
        std::getline(file, line, '\n');
        const std::size_t end = file.tellg();
        blocks.emplace_back(Block{start, end});
        start = end + 1;
    }
    return blocks;
}

Framework::blocks_of_pairs_t Framework::map(const std::filesystem::path& fpath, const input_blocks_t& blocks)
{
    std::vector<std::thread> mappers;
    mappers.reserve(m_num_of_mappers);
    blocks_of_pairs_t result(m_num_of_mappers, pairs_t{});
    for(std::size_t cntr{0}; cntr < m_num_of_mappers; ++cntr)
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
    auto cmp = [](const auto& l, const auto& r)
    {
        return std::lexicographical_compare(std::begin(l.first), std::end(l.first),
                                            std::begin(r.first), std::end(r.first));
    };

    // sort
    {
        auto sort = [&cmp](pairs_t& block)
        {
            block.sort(cmp);
        };
        std::vector<std::thread> sorters;
        sorters.reserve(m_num_of_mappers);
        for(std::size_t cntr{0}; cntr < m_num_of_mappers; ++cntr)
            sorters.emplace_back(std::thread{sort, std::ref(mapped[cntr])});
        for(auto& sorter:sorters)
            sorter.join();
    }


    // shuffle. Keep sorted
    {
        blocks_of_pairs_t shuffled{m_num_of_reducers, blocks_of_pairs_t::value_type{}};
        //shuffled.reserve(m_num_of_reducers);
        //auto cur{std::begin(*std::begin(mapped))->first};
        //shuffled.emplace_back(typename decltype(shuffled)::value_type(1, std::move(*std::begin(*std::begin(mapped)))));

        auto find = [&shuffled, &cmp](const pair_t& pair)
        {
            const auto& key{pair.first};
            for(auto bit{std::begin(shuffled)}; bit != std::end(shuffled); ++bit)
            {
                for(auto it{std::begin(*bit)}; it != std::end(*bit); ++it)
                {
                    if(it->first == key)
                        return std::make_pair(bit, it);
                    if(it->first > key)
                        break;
                }
            }

            auto where_to_copy{std::begin(shuffled)};
            for(auto block{std::begin(shuffled)}; block != std::end(shuffled); ++block)
            {
                if(block->size() < where_to_copy->size())
                    where_to_copy = block;
            }
            return std::make_pair(where_to_copy, std::upper_bound(std::begin(*where_to_copy), std::end(*where_to_copy), pair, cmp));
        };

        for(auto& mapped_block:mapped)
        {
            while(!mapped_block.empty())
            {
                auto first{std::begin(mapped_block)};
                auto pair{find(*first)};
                auto& shuffled_block{*pair.first};
                auto& where{pair.second};
                // last points to the first element in the mapped_block such that first->first < element.first
                // or to the mapped_block.end
                auto last{std::upper_bound(first, std::end(mapped_block), *first, cmp)};
                // transfer elements [first,last) of mapped_block into shuffled_block
                shuffled_block.splice(where, mapped_block, first, last);
            }
        }
        return shuffled;
    }
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

    auto find = [&merged_block, &cmp](const pair_t& pair)
    {
        const auto& key{pair.first};
        for(auto it{std::begin(merged_block)}; it != std::end(merged_block); ++it)
        {
            if(it->first == key)
                return it;
            if(it->first > key)
                break;
        }

        auto where_to_copy{std::begin(shuffled)};
        for(auto block{std::begin(shuffled)}; block != std::end(shuffled); ++block)
        {
            if(block->size() < where_to_copy->size())
                where_to_copy = block;
        }
        return std::upper_bound(std::begin(*where_to_copy), std::end(*where_to_copy), pair, cmp);
    };

    for(auto reduced_block{std::next(merged_block)}; reduced_block != std::end(im_result); ++reduced_block)
    {
        while(!reduced_block->empty())
        {
            auto first{std::begin(*reduced_block)};
            auto cmp = [](const auto& l, const auto& r)
            {
                return std::lexicographical_compare(std::begin(l.first), std::end(l.first),
                                                    std::begin(r.first), std::end(r.first));
            };
            // last points to the first element in the mapped_block such that first->first < element.first
            // or to the mapped_block.end
            auto last_into{std::upper_bound(std::begin(*merged_block), std::end(*merged_block), *first, cmp)};
            auto last_from{std::upper_bound(std::begin(*merged_block), std::end(*merged_block), *first, cmp)};
            // transfer elements [first,last) of reduced_block into merged_block
            merged_block->splice(last_into, *reduced_block, first, last_into);
        }
    }

    pairs_t result{};
    m_reducer(*merged_block, result);
    return result;
}
