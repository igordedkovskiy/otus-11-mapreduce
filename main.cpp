// homework #11 mapreduce

#include <iostream>

using boost::asio::ip::tcp;

int main(int argc, char* argv[])
{
    std::locale::global(std::locale(""));
    if(argc != 4)
    {
        std::cerr << "Usage: " << argv[0] << " <src> <mnum> <rnum>" << std::endl;
        return 1;
    }
    return 0;
}
