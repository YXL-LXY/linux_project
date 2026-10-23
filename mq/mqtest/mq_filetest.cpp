#include "../mqcommon/mq_helper.hpp"

int main()
{
    // mq::FileHelper helper("../mqcommon/logger.hpp");
    // DLOG("是否存在：%d", helper.exists());
    // DLOG("文件大小：%ld", helper.size());

    // mq::FileHelper tmp_helper("./aaa/bbb/ccc/tmp.hpp");
    // if (tmp_helper.exists() == false) {
    //     std::string path = mq::FileHelper::parentDirectory("./aaa/bbb/ccc/tmp.hpp");
    //     if (mq::FileHelper(path).exists() == false) {
    //         mq::FileHelper::createDirectory(path);
    //     }
    //     mq::FileHelper::createFile("./aaa/bbb/ccc/tmp.hpp");
    // }

    // std::string body;
    // helper.read(body);
    // tmp_helper.write(body);

    // mq::FileHelper tmp_helper("./aaa/bbb/ccc/tmp.hpp");
    // char str[16] = {0};
    // tmp_helper.read(str, 8, 11);
    // DLOG("[%s]", str);
    // tmp_helper.write("12345678901", 8, 11);
    // tmp_helper.rename("./aaa/bbb/ccc/test.hpp");
    // mq::FileHelper::removeFile("./aaa/bbb/ccc/test.hpp");

    mq::FileHelper::removeDirectory("./aaa");
    return 0;
}