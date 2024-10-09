vcpkg_download_distfile(archive
    URLS "http://www.atnf.csiro.au/people/mcalabre/WCS/wcslib-8.3.tar.bz2"
    FILENAME "wcslib-8.3.tar.bz2"
    SHA512 248518489431cbcba7a5df9e34a747e2a007128639d8ab655ceee35250e609d952fe466b67cdca5defe16d3e926730d00bfc5c362c369f74851cd88973b506ba
)


vcpkg_extract_source_archive(
    src
    ARCHIVE "${archive}"
    PATCHES
        001-modern-configs.patch
)

vcpkg_configure_make(
    SOURCE_PATH ${src}
    COPY_SOURCE
    OPTIONS
        --disable-flex
        --disable-fortran
        --without-pgplot
        --without-cfitsio)

vcpkg_install_make(MAKEFILE GNUmakefile)
vcpkg_fixup_pkgconfig()
vcpkg_install_copyright(FILE_LIST "${src}/COPYING")

file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
