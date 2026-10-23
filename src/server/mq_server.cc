#include "mq_broker.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

int main(int argc, char *argv[])
{
    // usage: mqserver [data_dir] [port]
    const std::string basedir = (argc > 1) ? argv[1] : "./data/";
    const int port = (argc > 2) ? std::atoi(argv[2]) : 8085;

    if (mq::FileHelper(basedir).exists() == false)
    {
        if (mq::FileHelper::createDirectory(basedir) == false)
        {
            std::cerr << "cannot create data directory: " << basedir << std::endl;
            return 1;
        }
    }

    mq::Server server(port, basedir);
    server.start();
    return 0;
}