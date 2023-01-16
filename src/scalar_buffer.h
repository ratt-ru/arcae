#include <vector>

#include <arrow/api.h>
#include <casacore/casa/Arrays/Vector.h>

template <typename T>
class ScalarBuffer : arrow::Buffer {
private:
    std::unique_ptr<T []> data;
    casacore::Vector<T> vector;

    static casacore::Vector<T> vector_factory(
        std::size_t nrows,
        const std::unique_ptr<T []> & data)
    {
        return casacore::Vector<T>(
            casacore::IPosition(1, nrows),
            data.get(),
            casacore::SHARE);
    }

public:
    ScalarBuffer(const casacore::ScalarColumn<T> & column) :
        data(std::make_unique<T []>(column.nrow())),
        vector(vector_factory(column.nrow(), data)),
        arrow::Buffer(reinterpret_cast<uint8_t *>(data.get()), column.nrow()*sizeof(T))
    {
        column.getColumn(vector);
    }
};
