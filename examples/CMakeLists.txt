# ============================================================
# Build configuration for the examples.
# ============================================================

add_executable(echo_callback callback_echo/main.cpp)
target_include_directories(echo_callback PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(echo_callback PRIVATE ${PROJECT_NAME})

add_executable(echo_coroutine coroutine_echo/main.cpp)
target_include_directories(echo_coroutine PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(echo_coroutine PRIVATE ${PROJECT_NAME})
