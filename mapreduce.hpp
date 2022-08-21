#pragma once

#include <cstdint>
#include <vector>
#include <list>
#include <unordered_map>
#include <filesystem>
#include <fstream>
#include <functional>
#include <thread>

namespace mapreduce
{

// Это MapReduce фреймворк.
// Он универсальный.
// Он может выполнять разные map_reduce задачи.
// Он просто обрабатывает какие-то данные с помощью каких-то функций в нескольких потоках.
// Он ничего не знает о задаче, которую решает.
// Здесь не должно быть кода, завязанного на конкретную задачу - определение длины префикса.
//
// С помощью этого фреймворка должны решаться разные задачи.
// Когда напишете это, попробуйте решить с помощью этого фреймворка все задачи, которые мы разбирали на лекции.
//
// Это наш самописный аналог hadoop mapreduce.
// Он на самом деле не работает с по-настоящему большими данными, потому что выполняется на одной машине.
// Но мы делаем вид, что данных много, и представляем, что наши потоки - это процессы на разных узлах.
//
// Ни один из потоков не должен полностью загружать свои данные в память или пробегаться по всем данным.
// Каждый из потоков обрабатывает только свой блок.
//
// На самом деле даже один блок данных не должен полностью грузиться в оперативку, а должен обрабатываться построчно.
// Но в домашней работе можем этим пренебречь и загрузить один блок в память одним потоком.
//
// Всё в этом файле - это рекомендация.
// Если что-то будет слишком сложно реализовать, идите на компромисс, пренебрегайте чем-нибудь.
// Лучше сделать что-нибудь, чем застрять на каком-нибудь моменте и не сделать ничего.
template<typename MapperT, typename ReducerT, typename KeyT>
class Framework
{
public:
    Framework(MapperT&& mapper, std::size_t num_of_mappers,
              ReducerT&& reducer, std::size_t num_of_reducers);

    void run(const std::filesystem::path& input, const std::filesystem::path& output);

private:
    struct Block
    {
        std::size_t m_start{0};
        std::size_t m_end{0};
    };

    using input_blocks_t = std::vector<Block>;
    input_blocks_t split_input(const std::filesystem::path& file_path);

    /// \returns num_of_mappers vectors of pairs
    using mapped_block_t = std::list<std::pair<KeyT, std::size_t>>;
    //using mapped_block_t = std::unordered_map<KeyT, std::size_t>;
    using mapped_t = std::vector<mapped_block_t>;
    mapped_t map(const input_blocks_t& blocks);

    /// \returns num_of_reducers vectors of pairs
    using shuffled_block_t = std::list<KeyT, std::size_t>;
    using shuffled_t = std::vector<shuffled_block_t>;
    shuffled_t shuffle(mapped_t& mapped);

    using reduced_t = shuffled_block_t;
    reduced_t reduce(const shuffled_t& shuffled);

    std::size_t m_num_of_mappers{0};
    std::size_t m_num_of_reducers{0};

    MapperT m_mapper;
    ReducerT m_reducer;
};

template<typename MapperT, typename ReducerT, typename Key>
Framework<MapperT, ReducerT, Key>::Framework(MapperT&& mapper, std::size_t num_of_mappers,
                                        ReducerT&& reducer, std::size_t num_of_reducers):
    m_mapper{std::forward<decltype(mapper)>(mapper)},
    m_num_of_mappers{num_of_mappers},
    m_reducer{std::forward<decltype(reducer)>(reducer)},
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
template<typename MapperT, typename ReducerT, typename Key>
void Framework<MapperT, ReducerT, Key>::run(const std::filesystem::path &input, const std::filesystem::path &output)
{
    // split input data into num_of_mappers blocks
    auto blocks {split_input(input)};

    // map
    map(blocks);

    // shuffle: sort and split into num_of_reducers blocks
    shuffle();

    // reduce
    {
        std::vector<std::thread> reducers;
        reducers.reserve(m_num_of_reducers);
        for(auto& block:blocks)
            reducers.emplace_back(std::thread{m_reducer, std::ref(block)});
        for(auto& reducer:reducers)
            reducer.join();
    }

}

// Эта функция не читает весь файл.
// Определяем размер файла в байтах.
// Делим размер на количество блоков - получаем границы блоков.
// Читаем данные только вблизи границ.
// Выравниваем границы блоков по границам строк.
template<typename MapperT, typename ReducerT, typename Key>
typename Framework<MapperT, ReducerT, Key>::input_blocks_t
Framework<MapperT, ReducerT, Key>::split_input(const std::filesystem::path& file_path)
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
        blocks.emplace_back({start, end});
        start = end + 1;
    }
    return blocks;
}

template<typename MapperT, typename ReducerT, typename Key>
typename Framework<MapperT, ReducerT, Key>::mapped_t
Framework<MapperT, ReducerT, Key>::map(const input_blocks_t& blocks)
{
    std::vector<std::thread> mappers;
    mappers.reserve(m_num_of_mappers);
    mapped_t result(m_num_of_mappers, mapped_block_t{});
    for(std::size_t cntr{0}; cntr < 0; ++cntr)
        mappers.emplace_back(std::thread{m_mapper, std::ref(blocks[cntr]), std::ref(result[cntr])});
    for(auto& mapper:mappers)
        mapper.join();
    return result;
}

template<typename MapperT, typename ReducerT, typename KeyT>
typename Framework<MapperT, ReducerT, KeyT>::shuffled_t
Framework<MapperT, ReducerT, KeyT>::shuffle(mapped_t& mapped)
{
    // sort
    auto cmp = [](const auto& l, const auto& r)
    {
        return (std::hash<decltype(l.first)>{}(l.first) <
                std::hash<decltype(r.first)>{}(r.first));
    };
    for(auto& block:mapped)
        block.sort(std::begin(block), std::end(block), cmp);

    // shuffle
    shuffled_t shuffled;
    shuffled.reserve(m_num_of_reducers);
    std::size_t num_of_pairs{0};
    for(const auto& block:mapped)
        num_of_pairs += block.size();
    const auto num_of_pairs_in_block{(num_of_pairs % m_num_of_reducers ?
                                      num_of_pairs / m_num_of_reducers :
                                      num_of_pairs / m_num_of_reducers + 1)};
    auto cur = std::begin(*std::begin(mapped))->first;
    shuffled.emplace_back(decltype(shuffled)::value_type(*std::begin(*std::begin(mapped))));
    for(auto& block:mapped)
    {
        for(auto it{std::begin(block)}; it != std::end(block);)
        {
            if(it->first == cur)
            {
                shuffled.back().emplace_back(*it);
                it = block.erase(it);
                continue;
            }
            if(shuffled.back().size() < num_of_pairs_in_block)
            {
                shuffled.back().emplace_back(*it);
                it = block.erase(it);
                continue;
            }
            shuffled.emplace_back(decltype(shuffled)::value_type(*it));
            it = block.erase(it);
        }
    }
    return shuffled;
}

template<typename MapperT, typename ReducerT, typename KeyT>
typename Framework<MapperT, ReducerT, KeyT>::reduced_t
Framework<MapperT, ReducerT, KeyT>::reduce(const shuffled_t& shuffled)
{
    std::vector<std::thread> reducers;
    reducers.reserve(m_num_of_reducers);
    reduced_t result(m_num_of_reducers, shuffled_block_t{});
    for(std::size_t cntr{0}; cntr < 0; ++cntr)
        reducers.emplace_back(std::thread{m_reducer, std::ref(shuffled[cntr]), std::ref(result[cntr])});
    for(auto& reducer:reducers)
        reducer.join();
    return result;
}

}
