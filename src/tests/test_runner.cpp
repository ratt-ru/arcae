#include <iostream>

#include "../casa_arrow.h"

int main(void) {
    auto result = open_table("/home/simon/data/WSRT_polar.MS_p0");
    std::cout << result.ValueOrDie()->ToString() << std::endl;
}
