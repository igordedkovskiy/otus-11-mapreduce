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


int main() {
    std::filesystem::path input("emails.txt");
    std::filesystem::path output("out");
    constexpr std::size_t num_of_mappers = 3;
    constexpr std::size_t num_of_reducers = 2;

    struct Block
    {
        std::size_t m_start{0};
        std::size_t m_end{0};
    };

    using key_t = std::string;
    using pair_t = std::pair<key_t, std::size_t>;
    using block_of_pairs_t = std::list<pair_t>;
    using pairs_t = std::vector<block_of_pairs_t>;

    //auto mapper = [](const std::filesystem::path& fpath, const Block& block, block_of_pairs_t& out)
    auto mapper = [](const std::filesystem::path& fpath, std::size_t start, std::size_t end, block_of_pairs_t& out)
    {
        ;
    };

    auto reducer = [](const block_of_pairs_t& in, block_of_pairs_t& out)
    {
        ;
    };

    mapreduce::Framework<decltype(mapper), decltype(reducer), key_t>
            mr{mapper, num_of_mappers, reducer, num_of_reducers};

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
