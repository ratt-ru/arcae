include(FindPython3)

find_package(Python COMPONENTS Interpreter)
message("Python_EXECUABLE=${Python_EXECUTABLE}")
message("Python_ROOT_DIR=${Python_ROOT_DIR}")

if(NOT DEFINED NUMPY_INCLUDE AND DEFINED ENV{NUMPY_INCLUDE})
    set(NUMPY_INCLUDE $ENV{NUMPY_INCLUDE})
endif()

if(NOT DEFINED NUMPY_INCLUDE AND Python_Interpreter_FOUND)
    execute_process(COMMAND ${Python_EXECUTABLE} -c "import numpy; print(numpy.get_include())"
        OUTPUT_VARIABLE NUMPY_INCLUDE
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

if(NOT DEFINED NUMPY_INCLUDE)
    message(FATAL_ERROR "Unable to find numpy include directory. Set NUMPY_INCLUDE")
endif()


if(NOT DEFINED PYARROW_INCLUDE AND DEFINED ENV{PYARROW_INCLUDE})
    set(PYARROW_INCLUDE $ENV{PYARROW_INCLUDE})
endif()

execute_process(COMMAND ${Python_EXECUTABLE} -c "import pyarrow; pyarrow.create_library_symlinks()")

if(NOT DEFINED PYARROW_INCLUDE AND Python_Interpreter_FOUND)
    execute_process(COMMAND ${Python_EXECUTABLE} -c "import pyarrow; print(pyarrow.get_include())"
                    OUTPUT_VARIABLE PYARROW_INCLUDE
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

if(NOT DEFINED PYARROW_INCLUDE)
    message(FATAL_ERROR "Unable to find pyarrow include directory. Set PYARROW_INCLUDE")
endif()

if(NOT DEFINED PYARROW_LIBDIRS AND DEFINED ENV{PYARROW_LIBDIRS})
    set(PYARROW_LIBDIRS $ENV{PYARROW_LIBDIRS})
endif()

if(NOT DEFINED PYARROW_LIBDIRS AND Python_Interpreter_FOUND)
    execute_process(COMMAND ${Python_EXECUTABLE} -c "import pyarrow; print(' '.join(pyarrow.get_library_dirs()))"
                    OUTPUT_VARIABLE PYARROW_LIBDIRS
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

if(NOT DEFINED PYARROW_LIBDIRS)
    message(FATAL_ERROR "Unable to find pyarrow library directories. Set PYARROW_LIBDIRS")
endif()

if(NOT DEFINED PYARROW_LIBS AND DEFINED ENV{PYARROW_LIBS})
    set(PYARROW_LIBS $ENV{PYARROW_LIBS})
endif()

if(NOT DEFINED PYARROW_LIBS AND Python_Interpreter_FOUND)
    execute_process(COMMAND ${Python_EXECUTABLE} -c "import pyarrow; print(' '.join(pyarrow.get_libraries()))"
                    OUTPUT_VARIABLE PYARROW_LIBS
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

if(NOT DEFINED PYARROW_LIBS)
    message(FATAL_ERROR "Unable to find pyarrow libraries. Set PYARROW_LIBS")
endif()

message(NUMPY_INCLUDE: "${NUMPY_INCLUDE}")
message(PYARROW_INCLUDE: "${PYARROW_INCLUDE}")
message(PYARROW_LIBDIRS: "${PYARROW_LIBDIRS}")
message(PYARROW_LIBS: "${PYARROW_LIBS}")
