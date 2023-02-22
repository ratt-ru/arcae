// Complex Number Extension Type
#pragma once

#include <arrow/extension_type.h>

std::shared_ptr<arrow::DataType> complex64();
std::shared_ptr<arrow::DataType> complex128();


class ComplexFloatArray : public arrow::ExtensionArray {
 public:
  using arrow::ExtensionArray::ExtensionArray;
};

class ComplexType : public arrow::ExtensionType {
public:
  explicit ComplexType(const std::shared_ptr<DataType>& storage_type)
    : ExtensionType(fixed_size_list(storage_type, 2)) {}

  std::shared_ptr<arrow::DataType> value_type(void) const {
    return arrow::internal::checked_cast<const arrow::FixedSizeListType*>(this->storage_type().get())->value_type();
  }
};

class ComplexFloatType : public ComplexType {
 public:
  explicit ComplexFloatType()
      : ComplexType(arrow::float32()) {}

  std::string name() const override {
    return "complex64";
  }

  std::string extension_name() const override {
    return "casa-arrow.complex64";
  }

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override {
    return std::make_shared<ComplexFloatArray>(data);
  }

  arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
      std::shared_ptr<arrow::DataType> storage_type,
      const std::string& serialized) const override {
    return complex64();
  };

  std::string Serialize() const override {
    return "";
  }
};


class ComplexDoubleArray : public arrow::ExtensionArray {
 public:
  using arrow::ExtensionArray::ExtensionArray;
};

class ComplexDoubleType : public ComplexType {
 public:
  explicit ComplexDoubleType()
      : ComplexType(arrow::float64()) {}

  std::string name() const override {
    return "complex128";
  }

  std::string extension_name() const override {
    return "casa-arrow.complex128";
  }

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override {
    return std::make_shared<ComplexFloatArray>(data);
  }

  arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
      std::shared_ptr<arrow::DataType> storage_type,
      const std::string& serialized) const override {
    return complex128();
  };

  std::string Serialize() const override {
    return "";
  }
};
