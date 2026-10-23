#include "mq_broker.hpp"

int main()
{
    mq::Server server(8085, "./data/");
    server.start();
    return 0;
}