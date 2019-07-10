# Copyright 2016 Proyectos y Sistemas de Mantenimiento SL (eProsima).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#if(NOT ((MSVC OR MSVC_IDE) AND EPROSIMA_INSTALLER))
#    include(${PROJECT_SOURCE_DIR}/cmake/common/gtest.cmake)
#    check_gtest()
#
#    if(GTEST_FOUND)
#        if(WIN32)
#            add_definitions(-D_WIN32_WINNT=0x0601)
#        endif()
#
#        set(QOSPOLICIESTESTS_SOURCE
#            QosPoliciesTests.cpp
#            ${PROJECT_SOURCE_DIR}/src/cpp/Domain.cpp
#            ${PROJECT_SOURCE_DIR}/src/cpp/participant/ParticipantImpl.cpp
#            ${PROJECT_SOURCE_DIR}/src/cpp/rtps/participant/RTPSParticipantImpl.cpp
#            ${PROJECT_SOURCE_DIR}/src/cpp/rtps/builtin/BuiltinProtocols.cpp
#            ${PROJECT_SOURCE_DIR}/src/cpp/rtps/builtin/discovery/endpoint/EDP.cpp
#            ${PROJECT_SOURCE_DIR}/src/cpp/rtps/builtin/data/ReaderProxyData.cpp
#            ${PROJECT_SOURCE_DIR}/src/cpp/rtps/builtin/data/WriterProxyData.cpp
#            ${PROJECT_SOURCE_DIR}/src/cpp/rtps/builtin/discovery/participant/PDP.cpp
#        )
#        add_executable(QosPoliciesTests ${QOSPOLICIESTESTS_SOURCE})
#        target_compile_definitions(QosPoliciesTests PRIVATE FASTRTPS_NO_LIB)
#        target_include_directories(QosPoliciesTests PRIVATE
#            ${GTEST_INCLUDE_DIRS}
#            ${PROJECT_SOURCE_DIR}/include
#            ${PROJECT_BINARY_DIR}/include
#        )
#        target_link_libraries(QosPoliciesTests ${GTEST_LIBRARIES})
#        add_gtest(QosPoliciesTests SOURCES QosPoliciesTests.cpp)
#    endif()
#endif()