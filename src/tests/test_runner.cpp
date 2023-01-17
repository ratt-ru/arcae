#include <iostream>

#include "../casa_arrow.h"

int main(void) {
    auto result = open_table("/home/simon/data/WSRT_polar.MS_p0");
    auto table = result.ValueOrDie();
    // std::cout << out["UVW"] << std::endl;
    //std::cout << result.ValueOrDie()->ToString() << std::endl;
    // std::cout << table->GetColumnByName("UVW")->ToString() << std::endl;
    // std::cout << table->GetColumnByName("TIME")->ToString() << std::endl;

    // std::cout << table->GetColumnByName("UVW")->length() << std::endl;
    // std::cout << table->GetColumnByName("TIME")->length() << std::endl;
    // std::cout << table->num_rows() << std::endl;
    std::cout << table->ToString() << std::endl;
}
