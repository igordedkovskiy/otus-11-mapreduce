#pragma once

#include <cstdint>
#include <vector>
#include <list>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <functional>
#include <thread>

template<typename T> struct TD;

namespace mapreduce
{

struct Block
{
    std::size_t m_start{0};
    std::size_t m_end{0};
};

// Это MapReduce фреймворк. Он универсальный и ничего не знает о задаче, которую решает.
// Здесь не должно быть кода, завязанного на конкретную задачу.
//
// С помощью этого фреймворка должны решаться разные задачи.
// Когда напишете это, попробуйте решить с помощью этого фреймворка все задачи, которые мы разбирали
// на лекции.
//
// Это наш самописный аналог hadoop mapreduce.
// Он на самом деле не работает с по-настоящему большими данными, потому что выполняется на одной
// машине. Но мы делаем вид, что данных много, и представляем, что наши потоки - это процессы на
// разных узлах.
//
// Ни один из потоков не должен полностью загружать свои данные в память или пробегаться по всем
// данным. Каждый из потоков обрабатывает только свой блок.
//
// На самом деле даже один блок данных не должен полностью грузиться в оперативку, а должен
// обрабатываться построчно. Но в домашней работе можем этим пренебречь и загрузить один блок в
// память одним потоком.
template<typename MapperT, typename ReducerT, typename KeyT>
class Framework
{
public:
    Framework(const MapperT& mapper, std::size_t num_of_mappers,
              const ReducerT& reducer, std::size_t num_of_reducers);

    void run(const std::filesystem::path& input, const std::filesystem::path& output);

    void set(const MapperT& mapper);
    void set(const ReducerT& reducer);

private:
    using input_blocks_t = std::vector<Block>;
    input_blocks_t split_input(const std::filesystem::path& file_path);

    using pair_t = std::pair<KeyT, std::size_t>;
    using block_of_pairs_t = std::list<pair_t>;
    using pairs_t = std::vector<block_of_pairs_t>;

    /// \returns num_of_mappers vectors of pairs
    pairs_t map(const std::filesystem::path& fpath, const input_blocks_t& blocks);

    /// \returns num_of_reducers vectors of pairs
    pairs_t shuffle(pairs_t& mapped);

    pairs_t reduce(const pairs_t& shuffled);

    std::size_t m_num_of_mappers{0};
    std::size_t m_num_of_reducers{0};

    MapperT  m_mapper;
    ReducerT m_reducer;
//    std::function<void(const std::filesystem::path&, const mapreduce::Block&, block_of_pairs_t&)>  m_mapper;
//    std::function<> m_reducer;
};

template<typename MapperT, typename ReducerT, typename KeyT>
Framework<MapperT, ReducerT, KeyT>::Framework(const MapperT& mapper, std::size_t num_of_mappers,
                                             const ReducerT& reducer, std::size_t num_of_reducers):
    //m_mapper{std::forward<decltype(mapper)>(mapper)},
    m_mapper{mapper},
    m_num_of_mappers{num_of_mappers},
    //m_reducer{std::forward<decltype(reducer)>(reducer)},
    m_reducer{reducer},
    m_num_of_reducers{num_of_reducers}
{}

// Создаём mappers_count потоков
// В каждом потоке читаем свой блок данных
// Применяем к строкам данных функцию mapper
// Сортируем результат каждого потока
// Результат сохраняется в файловую систему (представляем, что это большие данные)
// Каждый поток сохраняет результат в свой файл (представляем, что потоки выполняются на разных узлах)


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


// Создаём reducers_count потоков
// В каждом потоке читаем свой файл (выход предыдущей фазы)
// Применяем к строкам функцию reducer
// Результат сохраняется в файловую систему
//             (во многих задачах выход редьюсера - большие данные, хотя в нашей задаче можно написать функцию reduce так, чтобы выход не был большим)
template<typename MapperT, typename ReducerT, typename KeyT>
void Framework<MapperT, ReducerT, KeyT>::run(const std::filesystem::path& input,
                                             const std::filesystem::path& output)
{
    auto blocks {split_input(input)};
    auto mapped{map(input, blocks)};
    auto shuffled{shuffle(mapped)};
    auto result{reduce(shuffled)};
    // write into output
}

template<typename MapperT, typename ReducerT, typename KeyT>
void Framework<MapperT, ReducerT, KeyT>::set(const MapperT& mapper)
{
    m_mapper = mapper;
}

template<typename MapperT, typename ReducerT, typename KeyT>
void Framework<MapperT, ReducerT, KeyT>::set(const ReducerT& reducer)
{
    m_reducer = reducer;
}

// Эта функция не читает весь файл.
// Определяем размер файла в байтах.
// Делим размер на количество блоков - получаем границы блоков.
// Читаем данные только вблизи границ.
// Выравниваем границы блоков по границам строк.
template<typename MapperT, typename ReducerT, typename KeyT>
typename Framework<MapperT, ReducerT, KeyT>::input_blocks_t
Framework<MapperT, ReducerT, KeyT>::split_input(const std::filesystem::path& file_path)
{
    const auto fsize{std::filesystem::file_size(file_path)};
    const auto block_size{(fsize % m_num_of_mappers ?
                           fsize / m_num_of_mappers :
                           fsize / m_num_of_mappers + 1)};
    input_blocks_t blocks;
    blocks.reserve(m_num_of_mappers);
    std::fstream file{file_path.filename(), std::ios::in};
    std::string line;
    for(std::size_t start{0}; start < fsize;)
    {
        file.seekg(start + block_size);
        std::getline(file, line);
        const std::size_t end = file.tellg();
        blocks.emplace_back(Block{start, end});
        start = end + 1;
    }
    return blocks;
}

template<typename MapperT, typename ReducerT, typename KeyT>
typename Framework<MapperT, ReducerT, KeyT>::pairs_t
Framework<MapperT, ReducerT, KeyT>::map(const std::filesystem::path& fpath,
                                        const input_blocks_t& blocks)
{
    std::vector<std::thread> mappers;
    mappers.reserve(m_num_of_mappers);
    pairs_t result(m_num_of_mappers, block_of_pairs_t{});
    for(std::size_t cntr{0}; cntr < m_num_of_mappers; ++cntr)
        mappers.emplace_back(std::thread{m_mapper,
                                         std::ref(fpath),
                                         std::ref(blocks[cntr]),
                                         std::ref(result[cntr])});
    for(auto& mapper:mappers)
        mapper.join();
    return result;
}

template<typename MapperT, typename ReducerT, typename KeyT>
typename Framework<MapperT, ReducerT, KeyT>::pairs_t
Framework<MapperT, ReducerT, KeyT>::shuffle(pairs_t& mapped)
{
    // sort
    {
        auto sort = [](block_of_pairs_t& block)
        {
            auto cmp = [](const auto& l, const auto& r)
            {
                return (std::hash<decltype(l.first)>{}(l.first) <
                        std::hash<decltype(r.first)>{}(r.first));
            };
            block.sort(cmp);
        };
        std::vector<std::thread> sorters;
        sorters.reserve(m_num_of_mappers);
        for(std::size_t cntr{0}; cntr < m_num_of_mappers; ++cntr)
            sorters.emplace_back(std::thread{sort, std::ref(mapped[cntr])});
        for(auto& sorter:sorters)
            sorter.join();
    }


    // shuffle
    {
        pairs_t shuffled;
        shuffled.reserve(m_num_of_reducers);
        auto cur{std::begin(*std::begin(mapped))->first};
        shuffled.emplace_back(typename decltype(shuffled)::value_type(1, std::move(*std::begin(*std::begin(mapped)))));

        auto find = [&shuffled](const KeyT& key)
        {
            static auto cur = std::begin(shuffled);
            for(auto bit{std::begin(shuffled)}; bit != std::end(shuffled); ++bit)
            {
                for(auto it{std::begin(*bit)}; it != std::end(*bit); ++it)
                {
                    if(it->first == key)
                        return std::make_pair(bit, it);
                }
            }
            if(cur == std::end(shuffled))
                cur = std::begin(shuffled);
            auto ret = std::make_pair(cur, std::begin(*cur));
            ++cur;
            return ret;
        };

        for(auto& block:mapped)
        {
            for(auto first{std::begin(block)}; first != std::end(block);)
            {
                auto pair = find(first->first);
                auto& into_block = *pair.first;
                auto& where = pair.second;
                auto find_last = [&block, &first]()
                {
                    const auto key{first->first};
                    for(auto it{first}; it != std::end(block); ++it)
                    {
                        if(it->first != key)
                            return it;
                    }
                    return std::end(block);
                };
                into_block.splice(where, block, first, find_last());
            }
        }
        return shuffled;
    }
}

template<typename MapperT, typename ReducerT, typename KeyT>
typename Framework<MapperT, ReducerT, KeyT>::pairs_t
Framework<MapperT, ReducerT, KeyT>::reduce(const pairs_t& shuffled)
{
    std::vector<std::thread> reducers;
    reducers.reserve(m_num_of_reducers);
    pairs_t result(m_num_of_reducers, block_of_pairs_t{});
    for(std::size_t cntr{0}; cntr < m_num_of_reducers; ++cntr)
        reducers.emplace_back(std::thread{m_reducer,
                                          std::ref(shuffled[cntr]),
                                          std::ref(result[cntr])});
    for(auto& reducer:reducers)
        reducer.join();
    return result;
}

}
