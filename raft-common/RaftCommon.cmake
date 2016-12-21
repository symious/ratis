#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Common CMake utilities and configuration, shared by all Native components.
#

#
# Platform-specific prerequisite checks.
#

if(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    # Only 64-bit Java is supported.
    if(NOT JVM_ARCH_DATA_MODEL EQUAL 64)
        message(FATAL_ERROR "Unrecognised JVM_ARCH_DATA_MODEL '${JVM_ARCH_DATA_MODEL}'. "
          "A 64-bit JVM must be used on Solaris, make sure that one is installed and, "
          "if necessary, the MAVEN_OPTS environment variable includes '-d64'")
    endif()

    # Only gcc is suported for now.
    if(NOT(CMAKE_COMPILER_IS_GNUCC AND CMAKE_COMPILER_IS_GNUCXX))
        message(FATAL_ERROR "Only gcc is supported on Solaris")
    endif()
endif()

#
# Helper functions and macros.
#

# Add flags to all the CMake compiler variables
macro(raft_add_compiler_flags FLAGS)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${FLAGS}")
endmacro()

# Add flags to all the CMake linker variables.
macro(raft_add_linker_flags FLAGS)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${FLAGS}")
endmacro()

# Compile a library with both shared and static variants.
function(raft_add_dual_library LIBNAME)
    add_library(${LIBNAME} SHARED ${ARGN})
    add_library(${LIBNAME}_static STATIC ${ARGN})
    set_target_properties(${LIBNAME}_static PROPERTIES OUTPUT_NAME ${LIBNAME})
endfunction()

# Link both a static and a dynamic target against some libraries.
function(raft_target_link_dual_libraries LIBNAME)
    target_link_libraries(${LIBNAME} ${ARGN})
    target_link_libraries(${LIBNAME}_static ${ARGN})
endfunction()

# Set all the output directories to the same place.
function(raft_output_directory TGT DIR)
    set_target_properties(${TGT} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    set_target_properties(${TGT} PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    set_target_properties(${TGT} PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction()

# Set the target directories for dynamic and static builds.
function(raft_dual_output_directory TGT DIR)
    raft_output_directory(${TGT} "${DIR}")
    raft_output_directory(${TGT}_static "${DIR}")
endfunction()

# Alter the behavior of find_package and find_library so that we find only
# shared libraries with a given version suffix.  You should save
# CMAKE_FIND_LIBRARY_SUFFIXES before calling this function and restore it
# afterwards.  On Windows this function is a no-op.  Windows does not encode
# version number information information into library path names.
macro(raft_set_find_shared_library_version LVERS)
    if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        # Mac OS uses .dylib
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".${LVERS}.dylib")
    elseif(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
        # FreeBSD has always .so installed.
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    elseif(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
        # Windows doesn't support finding shared libraries by version.
    else()
        # Most UNIX variants use .so
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".so.${LVERS}")
    endif()
endmacro()

# Alter the behavior of find_package and find_library so that we find only
# shared libraries without any version suffix.  You should save
# CMAKE_FIND_LIBRARY_SUFFIXES before calling this function and restore it
# afterwards. On Windows this function is a no-op.  Windows does not encode
# version number information information into library path names.
macro(raft_set_find_shared_library_without_version)
    if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        # Mac OS uses .dylib
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".dylib")
    elseif(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
        # No effect
    else()
        # Most UNIX variants use .so
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    endif()
endmacro()

#
# Configuration.
#

# Initialise the shared gcc/g++ flags if they aren't already defined.
if(NOT DEFINED GCC_SHARED_FLAGS)
    set(GCC_SHARED_FLAGS "-g -O2 -Wall -pthread -D_FILE_OFFSET_BITS=64")
endif()

# Add in support other compilers here, if necessary,
# the assumption is that GCC or a GCC-compatible compiler is being used.

# Set the shared GCC-compatible compiler and linker flags.
raft_add_compiler_flags("${GCC_SHARED_FLAGS}")
raft_add_linker_flags("${LINKER_SHARED_FLAGS}")

#
# Linux-specific configuration.
#
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Make GNU extensions available.
    raft_add_compiler_flags("-D_GNU_SOURCE")

    # If JVM_ARCH_DATA_MODEL is 32, compile all binaries as 32-bit.
    if(JVM_ARCH_DATA_MODEL EQUAL 32)
        # Force 32-bit code generation on amd64/x86_64, ppc64, sparc64
        if(CMAKE_COMPILER_IS_GNUCC AND CMAKE_SYSTEM_PROCESSOR MATCHES ".*64")
            raft_add_compiler_flags("-m32")
            raft_add_linker_flags("-m32")
        endif()
        # Set CMAKE_SYSTEM_PROCESSOR to ensure that find_package(JNI) will use 32-bit libraries
        if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
            set(CMAKE_SYSTEM_PROCESSOR "i686")
        endif()
    endif()

    # Determine float ABI of JVM on ARM.
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
        find_program(READELF readelf)
        if(READELF MATCHES "NOTFOUND")
            message(WARNING "readelf not found; JVM float ABI detection disabled")
        else(READELF MATCHES "NOTFOUND")
            execute_process(
                COMMAND ${READELF} -A ${JAVA_JVM_LIBRARY}
                OUTPUT_VARIABLE JVM_ELF_ARCH
                ERROR_QUIET)
            if(NOT JVM_ELF_ARCH MATCHES "Tag_ABI_VFP_args: VFP registers")
                # Test compilation with -mfloat-abi=softfp using an arbitrary libc function
                # (typically fails with "fatal error: bits/predefs.h: No such file or directory"
                # if soft-float dev libraries are not installed)
                message("Soft-float JVM detected")
                include(CMakePushCheckState)
                cmake_push_check_state()
                set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -mfloat-abi=softfp")
                include(CheckSymbolExists)
                check_symbol_exists(exit stdlib.h SOFTFP_AVAILABLE)
                if(NOT SOFTFP_AVAILABLE)
                    message(FATAL_ERROR "Soft-float dev libraries required (e.g. 'apt-get install libc6-dev-armel' on Debian/Ubuntu)")
                endif()
                cmake_pop_check_state()
                raft_add_compiler_flags("-mfloat-abi=softfp")
            endif()
        endif()
    endif()

#
# Solaris-specific configuration.
#
elseif(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    # Solaris flags. 64-bit compilation is mandatory, and is checked earlier.
    raft_add_compiler_flags("-m64 -D_POSIX_C_SOURCE=200112L -D__EXTENSIONS__ -D_POSIX_PTHREAD_SEMANTICS")
    set(CMAKE_C_FLAGS "-std=gnu99 ${CMAKE_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "-std=gnu++98 ${CMAKE_CXX_FLAGS}")
    raft_add_linker_flags("-m64")

    # CMAKE_SYSTEM_PROCESSOR is set to the output of 'uname -p', which on Solaris is
    # the 'lowest' ISA supported, i.e. 'i386' or 'sparc'. However in order for the
    # standard CMake modules to look in the right places it needs to reflect the required
    # compilation mode, i.e. 64 bit. We therefore force it to either 'amd64' or 'sparcv9'.
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "i386")
        set(CMAKE_SYSTEM_PROCESSOR "amd64")
        set(CMAKE_LIBRARY_ARCHITECTURE "amd64")
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "sparc")
        set(CMAKE_SYSTEM_PROCESSOR "sparcv9")
        set(CMAKE_LIBRARY_ARCHITECTURE "sparcv9")
    else()
        message(FATAL_ERROR "Unrecognised CMAKE_SYSTEM_PROCESSOR ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
endif()
