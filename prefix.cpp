#include <filesystem>

#include "mapreduce.hpp"

// В этом файле находится клиентский код, который использует наш MapReduce фреймворк.
// Этот код знает о том, какую задачу мы решаем.
// Задача этого кода - верно написать мапер, редьюсер, запустить mapreduce задачу, обработать результат.
// Задача - найти минимальную длину префикса, который позволяет однозначно идентифицировать строку в файле.
// Задача не решается в одну mapreduce задачу. Нужно делать несколько запусков.
//
// Как предлагаю делать я:
// Выделяем первые буквы слов (в мапере), решаем для них задачу "определить, есть ли в них повторы".
// Если не прокатило, повторяем процедуру, выделяя первые две буквы.
// И т.д. В итоге найдём длину префикса, который однозначно определяет строку.
//
// Здесь описано то, как я примерно решал бы задачу, это не руководство к действию, а просто пояснение к основному тексту задания.
// Вы можете поступать по-своему (не как я описываю), задание творческое!
// Можете делать так, как написано, если считаете, что это хорошо.

using key_t = mapreduce::Framework::KeyT;
using pair_t = mapreduce::Framework::pair_t;
using pairs_t = mapreduce::Framework::pairs_t;
using blocks_of_pairs_t = mapreduce::Framework::blocks_of_pairs_t;

void mapper_(const std::filesystem::path& fpath, const mapreduce::Block& block, pairs_t& out)
{
    ;
}

int main() {
    std::filesystem::path input("emails.txt");
    std::filesystem::path output("out");
    constexpr std::size_t num_of_mappers = 3;
    constexpr std::size_t num_of_reducers = 2;


    auto mapper = [](const std::filesystem::path& fpath, const mapreduce::Block& block, pairs_t& out)
    {
//        auto common_prefix = [](std::string& s1, std::string& s2)
//        {
//            for(std::size_t cntr{0}; cntr < s1.size() && cntr < s2.size(); ++cntr)
//            {
//                if(s1[cntr] != s2[cntr])
//                    return s1.substr(cntr - 1);
//            }
//            return s1.size() <= s2.size() ? s1 : s2;
//        };

//        std::string s1, s2;
//        std::getline(s1);
//        std::string longest_common_prefix;
//        while()
//        {
//            std::getline(s2);
//            auto prefix = common_prefix(s1, s2);
//            if(prefix.size() > longest_common_prefix.size())
//                longest_common_prefix = std::move(prefix);
//            s1 = std::move(s2);
//        }
//        out.emplace_back(std::make_pair(std::move(longest_common_prefix), longest_common_prefix.size()));
    };

    auto reducer = [](const pairs_t& in, pairs_t& out)
    {
//        auto cmp = [](const pair_t& l, const pair_t& r){ return l.second < r.second; };
//        out.emplace_back(*std::minmax_element(std::begin(in), std::end(in), cmp));
    };

    mapreduce::Framework mr{mapper, num_of_mappers, reducer, num_of_reducers};

    //цикл по длине префикса
    {
//        mr.set_mapper([](){
//            // моё предложение:
//            //     * получает строку,
//            //     * выделяет префикс,
//            //     * возвращает пары (префикс, 1).
//        });
//        mr.set_reducer([](){
//            // моё предложение:
//            //     * получает пару (префикс, число),
//            //     * если текущий префикс совпадает с предыдущим или имеет число > 1, то возвращает false,
//            //     * иначе возвращает true.
//            //
//            // Почему тут написано "число", а не "1"?
//            // Чтобы учесть возможность добавления фазы combine на выходе мапера.
//            // Почитайте, что такое фаза combine в hadoop.
//            // Попробуйте это реализовать, если останется время.
//        });
        mr.run(input, output);
    }

    return 0;
}
